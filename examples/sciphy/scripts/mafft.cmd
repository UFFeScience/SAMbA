#!/bin/bash
export NAME=$1
perl "$WORKSPACE"/binaries/numberFasta.pl $NAME > $NAME.fastaNumbered
"$WORKSPACE"/binaries/mafft-7.313-linux/mafft-linux64/mafft $NAME.fastaNumbered > $NAME.mafft