#!/usr/bin/env bash
./build/mvn -Phadoop-2.7 -Dhadoop.version=2.7.3  -Pscala-2.12  -DskipTests -Dcheckstyle.skip=true   -Dscalastyle.skip=true compile install
