# Copyright (c) 2015, MapR Technologies
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


import six

import sahara.plugins.mapr.domain.node_process as np
import sahara.plugins.mapr.domain.service as s
import sahara.plugins.mapr.util.validation_utils as vu


SPARK_MASTER = np.NodeProcess(
    name='sparkmaster',
    ui_name='Spark-Master',
    package='mapr-spark-master',
    open_ports=[7077]
)

SPARK_HISTORY_SERVER = np.NodeProcess(
    name='sparkhistoryserver',
    ui_name='Spark-HistoryServer',
    package='mapr-spark-historyserver'
)

SPARK_SLAVE = np.NodeProcess(
    name='sparkmaster',
    ui_name='Spark-Slave',
    package='mapr-spark'
)

SPARK_MASTER_PORT = '7077'


@six.add_metaclass(s.Single)
class Spark(s.Service):
    SPARK_HOME_DIR = '/opt/mapr/spark/spark-1.1.0'
    SPARK_SLAVES_FILE = '%s/conf/slaves' % SPARK_HOME_DIR
    def __init__(self):
        super(Spark, self).__init__()
        self.name = 'spark'
        self.ui_name = 'Spark'
        self.version = '1.1.0'
        self.node_processes = [SPARK_HISTORY_SERVER, SPARK_MASTER, SPARK_SLAVE]
        self.dependencies = [('mapr-spark', self.version)]
        self.ui_info = [('SPARK', SPARK_MASTER, 'http://%s:8080')]
        self._validation_rules = [
            vu.exactly(1, SPARK_MASTER),
            vu.exactly(1, SPARK_HISTORY_SERVER),
            vu.at_least(1, SPARK_SLAVE),
        ]

    def _get_packages(self, node_processes):
        return [(np.get_package(), self.version)
                for np in node_processes
                if np is not SPARK_HISTORY_SERVER]

    def update(self, cluster_context, instances=None):
        self.set_worker_nodes_to_spark_master_config(cluster_context)

    def set_worker_nodes_to_spark_master_config(self, cluster_context):
        slaves_list = cluster_context.get_spark_slaves_hosts()
        spark_master_nodes = cluster_context.get_instances(SPARK_MASTER)
        slave_ips = '\n'.join(slaves_list)
        for instance in spark_master_nodes:
            with instance.remote() as remote:
                remote.write_file_to(self.SPARK_SLAVES_FILE, slave_ips,
                                     run_as_root=True, timeout=1800)