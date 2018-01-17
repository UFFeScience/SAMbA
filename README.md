# SciSpark
>An extension of Apache Spark for scientific computational experiments

## Overview

This repository presents a quick start guide for SciSpark, an extension over Apache Spark, which aims at running black-box native programs that handle raw data files. Furthermore, this engine collects, store, and query provenance and domain-specific data that were manipulated during the execution of scientific applications. With respect to the analytical capabilities, SciSpark provides runtime dataflow analysis based on the provenance traces.

## Presentation Video

To watch the video, please, click in the image below.
<a href="https://drive.google.com/file/d/1Zb1u1vswO5GNBOHRxBelNqrEfEc5aLWY/view" target="_blank">
![](SciSpark.png)
</a>
## Download: Docker image

We make SciSpark available for download through a [Docker](https://www.docker.com/) image. This image has all softwares requirements to run our applications using SciSpark and Apache Spark (our baseline). To download and run it, follow the steps below.

### Pull the image
To get the docker image, you need to pull it from docker hub. For this, run the command: 
```
docker pull thaylongs/sci-spark
```
### Configuration and Running the Image
After you pull the Docker image, now you can create a container from this image running the follow command:

```
docker run --rm -it \
       -p 8000:8000 -p 9042:9042 \
       -v $PWD/repository:/SciSparkFiles/SciSpark/gitblit/data/git \
       -v $PWD/database:/SciSparkFiles/datastax-ddc-3.9.0/data \
       -v $PWD:/home/scispark/workspace thaylongs/sci-spark
```

This command will share the current folder of the terminal (```$PWD:/workspace```) as the workspace of the container. So, in this folder should have the files that you want to use in your experiment, for example, source code and softwares. In our example command, inside the current directory will be created another two folders,  the ```$PWD/repository``` for save data from the git repository,  and the ```$PWD/database``` for save the database data. You can change these directories as you want. This command also opens the ports 8000 and 9042, for the web interface and the Cassandra database, respectively.

After you run the command, an bash in the workspace directory will be available for you run your codes. This container, the binary of Apache Spark and Scala already in the ```PATH``` environment variable. This environment also already has the following variables. 
- SPARK_HOME => The SciSpark root path.
- WORKSPACE  => The workspace path.
- SCI_SPARK_REPOSITORY => The cvs repository path.
- SCI_SPARK_CLASS_PATH => List of all jars of SciSpark/Spark, it is used to compile your source code. 

In this container, to compile a scala source code for create the .jar to submit to SciSpark, you run the follow command:

```
scalac -classpath "$SCI_SPARK_CLASS_PATH" SourceCode.scala -d TheOutput.jar
``` 

You also can run the scala interactive shell, with all SciSpark/Spark jars, running the follow command: 

```
scala -cp $SCI_SPARK_CLASS_PATH -J-Xmx1g
```

## Quick Start Guide
