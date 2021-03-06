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

import sahara.plugins.mapr.domain.configuration_file as bcf
import sahara.plugins.mapr.domain.node_process as np
import sahara.plugins.mapr.domain.service as s
import sahara.plugins.mapr.services.hive.hive as hive
import sahara.plugins.mapr.util.validation_utils as vu
import sahara.utils.files as files


IMPALA_SERVER = np.NodeProcess(
    name='impalaserver',
    ui_name='Impala-Server',
    package='mapr-impala-server',
    open_ports=[21000, 21050, 25000]
)
IMPALA_STATE_STORE = np.NodeProcess(
    name='impalastore',
    ui_name='Impala-Statestore',
    package='mapr-impala-statestore',
    open_ports=[25010]
)
IMPALA_CATALOG = np.NodeProcess(
    name='impalacatalog',
    ui_name='Impala-Catalog',
    package='mapr-impala-catalog',
    open_ports=[25020]
)

LOG = logging.getLogger(__name__)


class Impala(s.Service):
    def __init__(self):
        super(Impala, self).__init__()
        self.name = 'impala'
        self.ui_name = 'Impala'
        self.node_processes = [
            IMPALA_CATALOG,
            IMPALA_SERVER,
            IMPALA_STATE_STORE,
        ]

    def _get_impala_env_props(self, context):
        return {}

    def get_config_files(self, cluster_context, configs, instance=None):
        remote_path = "/opt/mapr/impala/impala-%s/conf/" % self.version
        defaults = 'plugins/mapr/services/impala/resources/impala-env.sh'

        impala_env = bcf.TemplateFile("env.sh")
        impala_env.remote_path = remote_path
        if instance:
            impala_env.fetch(instance)
        impala_env.parse(files.get_file_text(defaults))
        impala_env.add_properties(self._get_impala_env_props(cluster_context))

        return [impala_env]

    def post_install(self, cluster_context, instances):
        impalas = cluster_context.filter_instances(instances, IMPALA_SERVER)
        for instance in impalas:
            with instance.remote() as r:
                LOG.debug("Setting Impala home dir owner")
                r.execute_command('chown -R mapr:mapr /opt/mapr/impala',
                                  run_as_root=True)


@six.add_metaclass(s.Single)
class ImpalaV123(Impala):
    def __init__(self):
        super(ImpalaV123, self).__init__()
        self.version = '1.2.3'
        self.dependencies = [
            ('mapr-hive', hive.HiveV012().version),
            ('mapr-impala', self.version),
        ]
        self._validation_rules = [
            vu.depends_on(hive.HiveV012(), self),
            vu.on_same_node(IMPALA_CATALOG, hive.HIVE_SERVER_2),
        ]

    def _get_impala_env_props(self, context):
        return {
            'impala_version': self.version,
            'statestore_host': context.get_instance_ip(IMPALA_STATE_STORE),
            'catalog_host': context.get_instance_ip(IMPALA_CATALOG),
        }


@six.add_metaclass(s.Single)
class ImpalaV141(Impala):
    def __init__(self):
        super(ImpalaV141, self).__init__()
        self.version = '1.4.1'
        self.dependencies = [
            ('mapr-hive', hive.HiveV013().version),
            ('mapr-impala', self.version),
        ]
        self._validation_rules = [
            vu.depends_on(hive.HiveV013(), self),
            vu.on_same_node(IMPALA_CATALOG, hive.HIVE_SERVER_2),
        ]

    def _get_impala_env_props(self, context):
        return {
            'impala_version': self.version,
            'statestore_host': context.get_instance_ip(IMPALA_STATE_STORE),
            'catalog_host': context.get_instance_ip(IMPALA_CATALOG),
        }