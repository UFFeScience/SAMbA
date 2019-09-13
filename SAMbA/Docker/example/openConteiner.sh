#!/bin/bash

docker run --cap-add mknod --cap-add sys_admin --device=/dev/fuse \
           --privileged --rm -it -p 8000:8000 -p 9042:9042 \
           -v $PWD/repository:/SAMbAFiles/SAMbA/gitblit/data/git \
           -v $PWD/database:/SAMbAFiles/apache-cassandra/data \
           -v $PWD:/home/samba/workspace thaylongs/samba