#!/usr/bin/env bash
export ENABLE_PROVENANCE=FALSE
export ENABLE_VCS=FALSE

export MAVEN_OPTS="-Xss1500m"
./build/mvn -Phadoop-3.1   -Pscala-2.12 compile package