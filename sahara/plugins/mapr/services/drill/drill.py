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

import sahara.plugins.mapr.domain.node_process as np
import sahara.plugins.mapr.domain.service as s
import sahara.plugins.mapr.util.validation_utils as vu

LOG = logging.getLogger(__name__)

_CONFIGURE_SH_TIMEOUT = 600

DRILL = np.NodeProcess(
    name='drill',
    ui_name='Drill',
    package='mapr-drill',
    open_ports=[]
)


@six.add_metaclass(s.Single)
class Drill(s.Service):
    def __init__(self):
        super(Drill, self).__init__()
        self.name = 'drill'
        self.ui_name = 'Drill'
        self.version = '0.7'
        self.node_processes = [DRILL]
        self.ui_info = [('Drill', DRILL, 'http://%s:8047')]
        self._validation_rules = [vu.at_least(1, DRILL)]

    def install(self, cluster_context, instances):
        pass

    def post_start(self, cluster_context, instances):
        if not instances:
            instances = cluster_context.get_instances(
                DRILL.get_ui_name())

        super(Drill, self).install(cluster_context, instances)

        # command = cluster_context.get_configure_sh_path() + ' -R'
        for instance in instances:
            with instance.remote() as r:
                LOG.debug("Executing Re-configure.sh")
                # r.execute_command('sudo -i ' + command,
                #                   timeout=_CONFIGURE_SH_TIMEOUT,
                #                   raise_when_error=False)
                LOG.debug("Setting Drill home dir owner")
                r.execute_command('chown -R mapr:mapr /opt/mapr/drill',
                                  run_as_root=True)
