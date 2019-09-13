#!/bin/bash

CASSANDRA_VERSION="3.11.2"
SCALA_VERSION="2.11.8"
echo "Checking Cassandra $CASSANDRA_VERSION Files "
if [ ! -f "apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz" ]; then
    echo "Downloading Cassandra"
    wget "http://www-us.apache.org/dist/cassandra/$CASSANDRA_VERSION/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz"
fi

if [ ! -d "apache-cassandra" ]; then
    tar -xzf "apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz"
    mv "apache-cassandra-$CASSANDRA_VERSION" "apache-cassandra"
fi


echo "Checking Scala $SCALA_VERSION Files"
if [ ! -f "scala-$SCALA_VERSION.tgz" ]; then
    echo "Downloading Scala"
    wget "https://downloads.lightbend.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz"
fi

if [ ! -d "scala" ]; then
    tar -xzf "scala-$SCALA_VERSION.tgz"
    mv "scala-$SCALA_VERSION" scala
fi

echo "SAMbA ..."

filename=$(cd ../../ && ls spark-*-bin-SAMbA.tgz)
name=$(echo "$filename" | sed -r 's/.tgz//g')
rm -rf "$name"
rm -rf SAMbA

tar -xzf "../../$filename"
mv "$name" SAMbA

echo "SAMbA Web Interface"
rm -rf SAMbAWebApplication.jar
cp ../WebApplication/target/SAMbAWebApplication-*-SNAPSHOT.jar SAMbAWebApplication.jar

echo  "Configuring Files"

rm SAMbA/gitblit/data/git -rf
rm SAMbA/jars/spark-sql_*-SNAPSHOT.jar -rf

cd "apache-cassandra/conf/"

echo 'MAX_HEAP_SIZE="500M"' > cassandra-env.sh.temp
echo 'HEAP_NEWSIZE="300M"'  >> cassandra-env.sh.temp

cat cassandra-env.sh >> cassandra-env.sh.temp
rm cassandra-env.sh
mv cassandra-env.sh.temp cassandra-env.sh

cd ../../

echo "Build Docker Image"
docker build -t thaylongs/samba:latest .