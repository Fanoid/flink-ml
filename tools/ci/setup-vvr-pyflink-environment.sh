#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# This script would create a conda virtual environment and setup vvr pyflink
# libraries in it.
#
# Given that the executor of this script (e.g. CI pipeline) may not have
# permission to download the vvr flink repo, the flink repo needs to be
# downloaded in advance and its path should be passed to this script as an
# argument. For example:
#
# git clone http://gitlab.alibaba-inc.com/ververica/flink.git
# ./tools/ci/setup-vvr-pyflink-environment.sh ./flink

HERE="`dirname \"$0\"`"             # relative
HERE="`( cd \"$HERE\" && pwd )`"    # absolutized and normalized
if [ -z "$HERE" ] ; then
	exit 1
fi

FLINK_PATH=$1

# VVR binary distributions can be found on VVR Release Notes page.
# https://yuque.antfin-inc.com/ververica/release-notes/rul4lk
VVR_DIST_URL="http://vvr-daily.oss-cn-hangzhou-zmf.aliyuncs.com/vvr/20220929102706_510/1.15-vvr-6.0.2-2-SNAPSHOT-vvp-hadoop3.1.3.tar.gz"
VVR_DIST_FILE_NAME="1.15-vvr-6.0.2-2-SNAPSHOT-vvp-hadoop3.1.3"
VVR_FLINK_BRANCH="1.15-vvr-6.0.2-2"

# download flink's binary distribution and place it in flink-dist/target
# folder, so as to avoid the java compilation process
cd "${FLINK_PATH}"
git checkout $VVR_FLINK_BRANCH
mkdir -p ./flink-dist/target/flink-${VVR_FLINK_BRANCH}-SNAPSHOT-bin
cd ./flink-dist/target/flink-${VVR_FLINK_BRANCH}-SNAPSHOT-bin
wget -nv $VVR_DIST_URL
tar -xvf ${VVR_DIST_FILE_NAME}.tar.gz
mv ${VVR_DIST_FILE_NAME} flink-${VVR_FLINK_BRANCH}-SNAPSHOT

# set up environment like python3, conda
cd "${FLINK_PATH}"/flink-python
./dev/lint-python.sh -s all

CONDA_PATH="${FLINK_PATH}"/flink-python/dev/.conda/bin
"$CONDA_PATH"/conda init bash
source ~/.bashrc
"$CONDA_PATH"/conda activate python3.8

# build and install pyflink libraries
python -m pip install -r dev/dev-requirements.txt
python setup.py sdist

cd apache-flink-libraries
python setup.py sdist
python -m pip install dist/*.tar.gz
cd ..

python -m pip install dist/*.tar.gz

"$CONDA_PATH"/conda deactivate

echo "Conda virtual environment with pyflink ${VVR_FLINK_BRANCH}-SNAPSHOT has been setup."
echo "Use \"$CONDA_PATH/conda activate python3.8\" to activate the virtual environment."

