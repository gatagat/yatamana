#!/usr/bin/env bash

set -e
set -o pipefail

ml reset
ml use /groups/zimmer/software/171020-ii2/modules/all
ml load opencv/3.4.1-foss-2017a-python-2.7.13
ml load gcccore/6.3.0
ml load networkx/2.0-foss-2017a-python-2.7.13
ml load av/0.4.1-foss-2017a-python-2.7.13
ml load tifffile/0.12.1-foss-2017a-python-2.7.13
ml load scikit-image/0.14.0-foss-2017a-python-2.7.13
ml load numba/0.37.0-foss-2017a-python-2.7.13
ml load scikit-learn/0.19.1-foss-2017a-python-2.7.13
ml load scipy/1.0.1-foss-2017a-python-2.7.13
ml load tensorflow/1.5.0-foss-2017a-python-2.7.13
ml load sphinx/1.8.1-foss-2017a-python-2.7.13
ml list -t

SELF=$(cd $(dirname $0) && pwd)
export PYTHONPATH=$SELF/..:$SELF/../centerline/:$SELF/../wbfm-utils:$PYTHONPATH
echo "PYTHONPATH=$PYTHONPATH"

cd $SELF
rm -rf _build
sphinx-build -b html . _build
