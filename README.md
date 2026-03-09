# Connecting to Neo4j DB

To start neo4j:

```sh
cd ..
cd neo4j/bin
neo4j console
```

You will see something like:

```sh
Directories in use:
home:         C:\neo4j     
config:       C:\neo4j\conf
logs:         C:\neo4j\logs
plugins:      C:\neo4j\plugins
import:       C:\neo4j\import
data:         C:\neo4j\data
certificates: C:\neo4j\certificates
licenses:     C:\neo4j\licenses
run:          C:\neo4j\run
Starting Neo4j.
2026-03-09 07:23:20.973+0000 INFO  Logging config in use: File 'C:\neo4j\conf\user-logs.xml'
2026-03-09 07:23:20.991+0000 INFO  Starting...
2026-03-09 07:23:22.620+0000 INFO  This instance is ServerId{35c9207e} (35c9207e-ca10-4c8a-8836-aecc9b859638)
2026-03-09 07:23:24.625+0000 INFO  ======== Neo4j 5.26.21 ========
2026-03-09 07:23:27.808+0000 INFO  Anonymous Usage Data is being sent to Neo4j, see https://neo4j.com/docs/usage-data/
2026-03-09 07:23:27.943+0000 INFO  Bolt enabled on localhost:7687.
2026-03-09 07:23:29.385+0000 INFO  HTTP enabled on localhost:7474.
2026-03-09 07:23:29.386+0000 INFO  Remote interface available at http://localhost:7474/
2026-03-09 07:23:29.390+0000 INFO  id: removed-for-privacy
2026-03-09 07:23:29.391+0000 INFO  name: system
2026-03-09 07:23:29.391+0000 INFO  creationDate: 2026-03-08T23:02:21.998Z
2026-03-09 07:23:29.392+0000 INFO  Started.
```

In a web browser, you will see a welcome page. lease go to

```sh
http://localhost:7474/
```

and enter your credentials. You may leave the port as localhost:7687 in this demo.

There is a query bar at the top of the screen with

```sh
neo4j$
```

Please enter your query here. Press the blue play icon on the right hand side to complete the transaction.