#!/bin/bash
export NAME=$1
java -jar "$WORKSPACE"/binaries/modelgenerator/modelgenerator.jar $NAME.phylip 1 > $NAME.mg
python "$WORKSPACE"/binaries/modelgenerator/clean_modelgenerator.py $NAME.mg