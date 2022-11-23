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
import glob
import os
import shutil
import tempfile
import unittest
import uuid

from pyflink.common import RestartStrategies, Configuration
from pyflink.datastream import StreamExecutionEnvironment, HashMapStateBackend
from pyflink.java_gateway import get_gateway
from pyflink.table import StreamTableEnvironment
from pyflink.util.java_utils import get_j_env_configuration

from pyflink.ml.core.wrapper import JavaWithParams


def update_existing_params(target: JavaWithParams, source: JavaWithParams):
    get_gateway().jvm.org.apache.flink.ml.util.ReadWriteUtils \
        .updateExistingParams(target._java_obj, source._java_obj.getParamMap())


class PyFlinkMLTestCase(unittest.TestCase):
    def setUp(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.get_config().enable_object_reuse()
        self._load_dependency_jars()
        config = Configuration(
            j_configuration=get_j_env_configuration(self.env._j_stream_execution_environment))
        config.set_boolean("execution.checkpointing.checkpoints-after-tasks-finish.enabled", True)

        self.env.set_parallelism(4)
        self.env.enable_checkpointing(100)
        self.env.set_restart_strategy(RestartStrategies.no_restart())
        self.env.set_state_backend(HashMapStateBackend())
        self.t_env = StreamTableEnvironment.create(self.env)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _load_dependency_jars(self):
        from pyflink.ml.version import __version__

        flink_version = __version__.replace(".dev0", "-SNAPSHOT")
        this_directory = os.path.abspath(os.path.dirname(__file__))
        FLINK_ML_HOME = os.path.abspath(os.path.join(
            this_directory,
            "../../../../flink-ml-dist/target/flink-ml-%s-bin/flink-ml-%s" %
            (flink_version, flink_version)))
        FLINK_ML_LIB_PATH = os.path.join(FLINK_ML_HOME, "lib")
        if not os.path.isdir(FLINK_ML_LIB_PATH):
            raise Exception("You need to build Flink ML with maven you can run: "
                            "mvn -DskipTests clean package")

        for file in os.listdir(FLINK_ML_LIB_PATH):
            if file.endswith('.jar'):
                self.env.add_classpaths("file://{0}/{1}".format(FLINK_ML_LIB_PATH, file))

        # load flink-ml-lib/flink-ml-lib-*-tests.jar
        FLINK_ML_LIB_SOURCE_PATH = os.path.abspath(os.path.join(
            this_directory, "../../../../flink-ml-lib"))

        ml_test_jar = glob.glob(os.path.join(
            FLINK_ML_LIB_SOURCE_PATH, "target", "flink-ml-lib-*-tests.jar"))[0]

        self.env.add_classpaths("file://{0}".format(ml_test_jar))

    def save_and_reload(self, stage):
        path = os.path.join(self.temp_dir, 'test_save_and_reload', str(uuid.uuid1()))
        stage.save(path)
        try:
            self.env.execute('save_stage')
        except Exception as e:
            if "No operators defined in streaming topology." in str(e):
                pass
            else:
                raise e
        load_func = getattr(stage, 'load')
        return load_func(self.t_env, path)

    def assertListAlmostEqual(self, expected_list, actual_list, places=None, msg=None,
                              delta=None):
        self.assertEqual(len(expected_list), len(actual_list))
        for i in range(len(expected_list)):
            self.assertAlmostEqual(expected_list[i], actual_list[i],
                                   places=places, msg=msg, delta=delta)
