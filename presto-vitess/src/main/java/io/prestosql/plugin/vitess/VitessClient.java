/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.vitess;

import com.google.common.collect.ImmutableSet;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.Statement;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.Locale.ENGLISH;

public class VitessClient
        extends BaseJdbcClient
{
    private String vttabletSchemaName;
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    // TODO: Support jsonColumn
    private final Type jsonType;

    @Inject
    public VitessClient(BaseJdbcConfig config, VitessConfig vitessConfig, TypeManager typeManager)
            throws SQLException
    {
        super(config, "`", connectionFactory(config, vitessConfig));
        this.vttabletSchemaName = String.valueOf(vitessConfig.getVttabletSchemaName());
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, VitessConfig vitessConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (vitessConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(vitessConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(vitessConfig.getMaxReconnects()));
        }
        if (vitessConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(vitessConfig.getConnectionTimeout().toMillis()));
        }

        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("information_schema") && !schemaName.equalsIgnoreCase("performance_schema") && !schemaName.equals("mysql")) {
                    schemaNames.add(schemaName);
                }
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        // Abort connection before closing. Without this, the MySQL driver
        // attempts to drain the connection by reading all the results.
        connection.abort(directExecutor());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(Statement.class)) {
            statement.unwrap(Statement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String vttabletSchemaName = this.vttabletSchemaName;
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                vttabletSchemaName,
                null,
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = this.vttabletSchemaName;
            String jdbcTableName = schemaTableName.getTableName();

            log.info(vttabletSchemaName + " " + jdbcSchemaName);

            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, Optional.of(jdbcSchemaName), Optional.of(jdbcTableName))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();

                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            schemaTableName,
                            jdbcSchemaName,
                            null,
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return Optional.of(getOnlyElement(tableHandles));
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected Function<String, String> tryApplyLimit(OptionalLong limit)
    {
        if (!limit.isPresent()) {
            return Function.identity();
        }
        return limitFunction()
                .map(limitFunction -> (Function<String, String>) sql -> limitFunction.apply(sql, limit.getAsLong()))
                .orElseGet(Function::identity);
    }

    @Override
    public boolean supportsLimit()
    {
        return limitFunction().isPresent();
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.empty();
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        throw new PrestoException(JDBC_ERROR, "limitFunction() is implemented without isLimitGuaranteed()");
    }

    //    @Override
//    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
//            throws SQLException
//    {
//        return new QueryBuilder(identifierQuote).buildSql(
//                this,
//                connection,
//                null,
//                split.getSchemaName(),
//                split.getTableName(),
//                columnHandles,
//                split.getTupleDomain());
//    }

//    @Override
//    protected String toSqlType(Type type)
//    {
//        if (REAL.equals(type)) {
//            return "float";
//        }
//        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
//            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
//        }
//        if (TIMESTAMP.equals(type)) {
//            return "datetime";
//        }
//        if (VARBINARY.equals(type)) {
//            return "mediumblob";
//        }
//        if (isVarcharType(type)) {
//            VarcharType varcharType = (VarcharType) type;
//            if (varcharType.isUnbounded()) {
//                return "longtext";
//            }
//            if (varcharType.getBoundedLength() <= 255) {
//                return "tinytext";
//            }
//            if (varcharType.getBoundedLength() <= 65535) {
//                return "text";
//            }
//            if (varcharType.getBoundedLength() <= 16777215) {
//                return "mediumtext";
//            }
//            return "longtext";
//        }
//
//        return super.toSqlType(type);
//    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

//        if (jdbcTypeName.equalsIgnoreCase("json")) {
//            return Optional.of(jsonColumnMapping());
//        }
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (REAL.equals(type)) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (TIMESTAMP.equals(type)) {
            // TODO use `timestampWriteFunction`
            return WriteMapping.longMapping("datetime", timestampWriteFunctionUsingSqlTimestamp(session));
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("mediumblob", varbinaryWriteFunction());
        }
        if (isVarcharType(type)) {
            String dataType;
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                dataType = "longtext";
            }
            if (varcharType.getBoundedLength() <= 255) {
                dataType = "tinytext";
            }
            if (varcharType.getBoundedLength() <= 65535) {
                dataType = "text";
            }
            if (varcharType.getBoundedLength() <= 16777215) {
                dataType = "mediumtext";
            }
            else {
                dataType = "longtext";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        return super.toWriteMapping(session, type);
    }
}
