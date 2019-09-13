#!/usr/bin/env bash

echo "Startig Cassandra Data Base and wait 20 seconds"
/SAMbAFiles/apache-cassandra/bin/cassandra &> /tmp/logCassandra.txt

sleep 20


if [ ! -d "/SAMbAFiles/apache-cassandra/data/data/dfanalyzer" ]; then
  echo "Create the database in Cassandra"
  /SAMbAFiles/apache-cassandra/bin/cqlsh < $SPARK_HOME/CassandraDatabaseScript.cql
fi

