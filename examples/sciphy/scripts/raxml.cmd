#!/bin/bash
export PHYLIP=$1
export MG=$2
export PATH="$WORKSPACE"/binaries/RAxML-7.2.2/:$PATH
python "$WORKSPACE"/binaries/RAxML-7.2.2/execute_raxml.py . $PHYLIP $MG 5 4