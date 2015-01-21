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


import logging

import six

from sahara.i18n import _LI
from sahara.plugins.mapr.base import base_node_manager
import sahara.plugins.mapr.services.spark.spark as spark

LOG = logging.getLogger(__name__)


class SparkStarter(base_node_manager.BaseNodeManager):
    SPARK_HOME_DIR = '/opt/mapr/spark/spark-1.1.0'
    SPARK_SLAVES_FILE = '%s/conf/slaves' % SPARK_HOME_DIR
    SPARK_SLAVES_START_SCRIPT = '%s/sbin/start-slaves.sh' % SPARK_HOME_DIR
    DELAY_INTERVAL = 120

    def start(self, cluster_context, instances=None):
        self.edit_spark_env_sh(cluster_context)
        self.add_ha_to_env_sh(cluster_context)

        super(SparkStarter, self).start(cluster_context, instances)

        if cluster_context.is_spark_hs_enabled():
            self.create_hadoop_spark_dirs(cluster_context)
            self.install_spark_history_server(cluster_context)

        self.set_worker_nodes_to_spark_master_config(cluster_context)
        self.install_ssh_keys_to_nodes(cluster_context)
        if cluster_context.is_spark_hs_enabled():
            self.start_spark_history_servers(cluster_context)
        self.start_spark_masters(cluster_context)
        self.start_slaves_nodes(cluster_context)

    def set_worker_nodes_to_spark_master_config(self, cluster_context):
        slaves_list = cluster_context.get_spark_slaves_hosts()
        spark_master_nodes = cluster_context.get_instances(
            spark.SPARK_MASTER)
        slave_ips = '\n'.join(slaves_list)
        for instance in spark_master_nodes:
            with instance.remote() as remote:
                remote.write_file_to(self.SPARK_SLAVES_FILE, slave_ips,
                                     run_as_root=True, timeout=1800)

    def start_slaves_nodes(self, cluster_context):
        LOG.info(_LI('Starting Spark Workers'))
        spark_slave_node = cluster_context.get_instance(
            spark.SPARK_MASTER.get_ui_name())
        with spark_slave_node.remote() as remote:
            remote.execute_command(
                'sudo -u mapr ' + self.SPARK_SLAVES_START_SCRIPT, timeout=1800)

    def create_hadoop_spark_dirs(self, cluster_context):
        spark_master_instance = cluster_context.get_instance(
            spark.SPARK_MASTER.get_ui_name())
        with spark_master_instance.remote() as remote:
            remote.execute_command('sudo -u mapr hadoop fs -mkdir '
                                   '-p /apps/spark')
            remote.execute_command(
                'sudo -u mapr hadoop fs -chmod 777 /apps/spark')

    def install_spark_history_server(self, cluster_context):
        spark_hs_instances = cluster_context.get_instances(
            spark.SPARK_HISTORY_SERVER)
        exec_str = cluster_context.distro.create_install_cmd(
            [(spark.SPARK_HISTORY_SERVER.get_package(), None)])
        for instance in spark_hs_instances:
            with instance.remote() as r:
                r.execute_command(exec_str, run_as_root=True, timeout=1800)

    def install_ssh_keys_to_nodes(self, cluster_context):

        slaves_nodes = cluster_context.get_instances(
            spark.SPARK_SLAVE.get_ui_name())
        master_nodes = cluster_context.get_instances(
            spark.SPARK_MASTER.get_ui_name())

        spark_nodes_list = master_nodes
        spark_nodes_list.extend(
            node for node in slaves_nodes if node not in spark_nodes_list)
        private_key = cluster_context.cluster.management_private_key
        public_key = cluster_context.cluster.management_public_key
        key_files = {
            '/home/mapr/.ssh/id_rsa': private_key,
            '/home/mapr/.ssh/authorized_keys': public_key,
        }

        key_cmd = ('chown mapr:mapr /home/mapr/.ssh/*; '
                   'chmod 600 /home/mapr/.ssh/*')

        mkdir_cmd = (
            'mkdir -p /home/mapr/.ssh && chown -R  mapr:mapr /home/mapr')

        for node in spark_nodes_list:
            with node.remote() as r:
                if not self._check_ssh_dir(r):
                    r.execute_command(mkdir_cmd, run_as_root=True,
                                      timeout=1800)
                r.write_files_to(key_files, run_as_root=True, timeout=1800)
                r.execute_command(key_cmd, run_as_root=True, timeout=1800)

    def _check_ssh_dir(self, remote):
        ec, out = remote.execute_command("[ -d /home/mapr/.ssh ]",
                                         run_as_root=True,
                                         raise_when_error=False)
        return ec == 0

    def edit_spark_env_sh(self, cluster_context):
        instances = cluster_context.get_instances()
        master = cluster_context.get_instance(spark.SPARK_MASTER)
        LOG.debug('Set SparkMaster IP to env.sh')
        for instance in instances:
            with instance.remote() as r:
                r.replace_remote_string(
                    '/opt/mapr/spark/spark-1.1.0/conf/spark-env.sh',
                    'export SPARK_MASTER_IP=.*',
                    'export SPARK_MASTER_IP=%s' % master.fqdn())

    def start_spark_masters(self, cluster_context):
        LOG.info(_LI('Starting Spark Masters'))
        master_nodes = cluster_context.get_instances(
            spark.SPARK_MASTER)
        start_master_cmd = (
            'sudo -u mapr /opt/mapr/spark/spark-1.1.0/sbin/start-master.sh')
        for master in master_nodes:
            with master.remote() as r:
                r.execute_command(start_master_cmd)

    def start_spark_history_servers(self, cluster_context):
        LOG.info(_LI('Starting Spark History Server'))
        master = cluster_context.get_instance(
            spark.SPARK_MASTER)
        start_script = 'start-history-server.sh'
        stop_script = 'stop-history-server.sh'
        stop_hs_cmd = (
            '/opt/mapr/spark/spark-1.1.0/sbin/%s' % stop_script)
        start_hs_cmd = (
            'sudo -u mapr /opt/mapr/spark/spark-1.1.0/sbin/%s' % start_script)
        if cluster_context.is_spark_hs_enabled():
            with master.remote() as r:
                r.execute_command(stop_hs_cmd, run_as_root=True)
                r.execute_command('sudo -u mapr ' + stop_hs_cmd)
                r.execute_command(start_hs_cmd)

    def add_ha_to_env_sh(self, cluster_context):
        LOG.debug('Add Spark HA string to env.sh')
        zookeepers = cluster_context.get_zookeeper_nodes_ip_with_port()

        opts = {
            'spark.deploy.recoveryMode': 'ZOOKEEPER',
            'spark.deploy.zookeeper.url': '%s' % zookeepers,
            'zookeeper.sasl.client': 'false',
            'java.security.auth.login.config': '/opt/mapr/conf/mapr.login.conf'
        }
        opts = ' '.join(map(lambda i: '-D%s=%s' % i, six.iteritems(opts)))
        opts = 'export SPARK_DAEMON_JAVA_OPTS="%s"\n' % opts

        masters = cluster_context.get_instances(spark.SPARK_MASTER)
        spark_env = '/opt/mapr/spark/spark-1.1.0/conf/spark-env.sh'
        for master in masters:
            with master.remote() as r:
                r.append_to_file(
                    spark_env, opts, run_as_root=True, timeout=1800)
