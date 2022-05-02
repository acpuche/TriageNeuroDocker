#!/bin/bash

# <INPUTDIR> corresponds to the /input directory
# <BASEDIR> corresponds to the /output directory
#

INPUTDIR="$1"
BASEDIR="$2"

bash mr_triage.sh ${INPUTDIR} ${BASEDIR} 1
echo "$(pwd)"
python main.py ${INPUTDIR} ${BASEDIR}
