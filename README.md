# Presto connector plugin for Vitess

**[!WARNING]**: This repo is forked from [vitessio/contrib: Vitess contributions](https://github.com/vitessio/contrib)

Catalog format (every line is required):

```
connector.name=vitess  
connection-url=jdbc:mysql://<vtgate ip address>:<vtgate port>/<Vitess keyspace>  
connection-user=<vtgate user>  
connection-password=<vtgate password>  
vitess.vttablet_schema_name=<schema name inside vttablet>  
```

- Vitess keyspace name is required in the connection URL, otherwise Presto can't access vttablet's `information_schema`  
- If not explicitly set by `-init_db_name_override` option or if [vttablet is _not_ managing another remote `mysqld`](https://vitess.io/docs/user-guides/vttablet-modes/#unmanaged-or-remote-mysql), vttablet's schema name is by default `vt_<keyspace name>`  
- This connector cannot list tables using `show tables` yet.



## Building

_(optional)_ Deploy Presto by following [the guide on prestosql.io](https://prestosql.io/docs/current/installation/deployment.html).

Clone [presto github repository](https://github.com/prestosql/presto/).

Checkout branch 315
```
git checkout 315
```

Change `pom.xml` found on presto repo's base directory:

- add `<module>presto-vitess</module>` to `<modules>` scope, and 
- add the following to `<dependencies>` scope:
```
            <dependency>
                <groupId>io.prestosql</groupId>
                <artifactId>presto-vitess</artifactId>
                <version>${project.version}</version>
            </dependency>
```

Clone this repository and copy the directory `presto-vitess` into prestodb repository's base directory.

Build the Vitess connector plugin using `mvnw` found on prestodb repo's base directory: 
```
./mvnw clean install -pl presto-vitess -am -DskipTests
```

Copy the directory `presto-vitess/target/presto-vitess-315` into your Presto deployment's  `plugin` directory.

---

Open-sourced by Bukalapak.
