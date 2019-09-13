#!/usr/bin/env bash
#./dev/make-distribution.sh --name custom-spark --tgz -Phadoop-2.7 -Phive -Phive-thriftserver -Pmesos
./dev/make-distribution.sh --name SAMbA --tgz -Phadoop-2.7 -Dhadoop.version=2.7.3  -Pscala-2.11 -Pmesos
