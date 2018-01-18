#!/bin/bash
export NAME=$1
export MAFFT_FILE=$2

java -Xms1024m -Xmx1024m -jar "$WORKSPACE"/binaries/readseq.jar -all -f=12 $MAFFT_FILE -o $NAME.phylip
java -Xms1024m -Xmx1024m -jar "$WORKSPACE"/binaries/readseq.jar -all -f=17 $MAFFT_FILE -o $NAME.nexus