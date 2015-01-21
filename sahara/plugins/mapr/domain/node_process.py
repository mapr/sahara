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


WARDEN_MANAGED_CMD = ('sudo -u mapr maprcli node services'
                      ' -name %(service)s'
                      ' -action %(action)s'
                      ' -nodes %(nodes)s')
SYSTEM_MANAGED_CMD = None
START_ACTION = 'start'
STOP_ACTION = 'stop'
RESTART_ACTION = 'restart'


class NodeProcess(object):
    def __init__(self, name, ui_name, package, open_ports=[], cli_name=None):
        self.name = name
        self.ui_name = ui_name
        self.package = package
        self.cli_name = cli_name
        self.open_ports = open_ports

    def get_name(self):
        return self.name

    def get_ui_name(self):
        return self.ui_name

    def get_package(self):
        return self.package

    def get_cli_name(self):
        return self.cli_name

    def start(self, instances):
        self.execute_action(instances, START_ACTION)

    def restart(self, instances):
        self.execute_action(instances, RESTART_ACTION)

    def stop(self, instances):
        self.execute_action(instances, STOP_ACTION)

    def execute_action(self, instances, action):
        nodes = ','.join(map(lambda i: i.management_ip, instances))
        args = {'service': self.name, 'action': action, 'nodes': nodes}
        command = WARDEN_MANAGED_CMD % args
        with instances[0].remote() as r:
            r.execute_command(command)

    def get_open_ports(self):
        return self.open_ports

    def __repr__(self):
        return '<Node process %s>' % self.ui_name
