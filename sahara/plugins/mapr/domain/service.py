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


import json
import logging

from sahara import context
from sahara.i18n import _LI
import sahara.plugins.general.exceptions as ex
import sahara.plugins.provisioning as p
from sahara.utils import files as files


_INSTALL_PACKAGES_TIMEOUT = 3600

LOG = logging.getLogger(__name__)


class Service(object):
    def __init__(self):
        self._name = None
        self._ui_name = None
        self._node_processes = frozenset()
        self._version = None
        self._dependencies = {}
        self._ui_info = []
        self._cluster_defaults = []
        self._node_defaults = []
        self._validation_rules = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def ui_name(self):
        return self._ui_name

    @ui_name.setter
    def ui_name(self, value):
        self._ui_name = value

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, value):
        self._version = value

    @property
    def node_processes(self):
        return self._node_processes

    @node_processes.setter
    def node_processes(self, iterable):
        def set_service(node_process):
            node_process.service = self
            return node_process

        self._node_processes = frozenset(map(set_service, iterable))

    @property
    def dependencies(self):
        return self._dependencies

    @dependencies.setter
    def dependencies(self, value):
        self._dependencies = value

    @property
    def ui_info(self):
        return self._ui_info

    @ui_info.setter
    def ui_info(self, iterable):
        self._ui_info = iterable

    @property
    def cluster_defaults(self):
        return self._cluster_defaults

    @cluster_defaults.setter
    def cluster_defaults(self, defaults):
        self._cluster_defaults = defaults

    @property
    def node_defaults(self):
        return self._node_defaults

    @node_defaults.setter
    def node_defaults(self, defaults):
        self._node_defaults = defaults

    def get_node_processes(self):
        return self.node_processes

    def get_versions(self):
        return self._version

    @property
    def validation_rules(self):
        return self._validation_rules

    def install(self, cluster_context, instances):
        LOG.info(
            _LI('START: Installing %(service)s on cluster=%(cluster)s'),
            {'service': self._ui_name, 'cluster': cluster_context.cluster.name}
        )
        with context.ThreadGroup() as tg:
            for instance in instances:
                tg.spawn('install-packages-%s' % instance.id,
                         self._install_packages_on_instance, cluster_context,
                         instance)
        LOG.info(_LI('END: Installing packages on cluster=%s'),
                 cluster_context.cluster.name)

    def _install_packages_on_instance(self, cluster_context, instance):
        processes = [p for p in self.get_node_processes() if
                     p.get_ui_name() in instance.node_group.node_processes]
        if processes is not None and len(processes) > 0:
            LOG.info(
                _LI('START: Installing %(service)s on %(instance)s'),
                {'service': self.ui_name, 'instance': instance.management_ip}
            )
            packages = self._get_packages(processes)
            cmd = cluster_context.distro.create_install_cmd(packages)
            with instance.remote() as r:
                r.execute_command(cmd, run_as_root=True,
                                  timeout=_INSTALL_PACKAGES_TIMEOUT,
                                  raise_when_error=False)
            LOG.info(_LI('END: Installing packages on instance=%s'),
                     instance.management_ip)

    def _get_packages(self, node_processes):
        result = []

        result += self.dependencies
        result += [(np.get_package(), self.version) for np in node_processes]

        return result

    def post_install(self, cluster_context, instances):
        pass

    def post_start(self, cluster_context, instances):
        pass

    def configure(self, cluster_context, instances=None):
        pass

    def update(self, cluster_context, instances=None):
        pass

    def get_file_path(self, file_name):
        template = 'plugins/mapr/services/%(service)s/resources/%(file_name)s'
        args = {'service': self.name, 'file_name': file_name}
        return template % args

    def get_configs(self):
        result = []

        for d_file in self.cluster_defaults:
            data = self._load_config_file(self.get_file_path(d_file))
            result += [self._create_config_obj(c, self.ui_name) for c in data]

        for d_file in self.node_defaults:
            data = self._load_config_file(self.get_file_path(d_file))
            result += [self._create_config_obj(c, self.ui_name, scope='node')
                       for c in data]

        return result

    def get_configs_dict(self):
        result = dict()
        for conf_obj in self.get_configs():
            result.update({conf_obj.name: conf_obj.default_value})
        return {self.ui_name: result}

    def _load_config_file(self, file_path=None):
        return json.loads(files.get_file_text(file_path))

    def get_config_files(self, cluster_context, configs, instance=None):
        return []

    def _create_config_obj(self, item, target='general', scope='cluster',
                           high_priority=False):
        def _prepare_value(value):
            if isinstance(value, str):
                return value.strip().lower()
            return value

        conf_name = _prepare_value(item.get('name', None))

        conf_value = _prepare_value(item.get('value', None))

        if not conf_name:
            raise ex.HadoopProvisionError(_LI("Config missing 'name'"))

        if conf_value is None:
            raise ValueError(
                "Config '%s' missing 'value'" % conf_name)

        if high_priority or item.get('priority', 2) == 1:
            priority = 1
        else:
            priority = 2

        return p.Config(
            name=conf_name,
            applicable_target=target,
            scope=scope,
            config_type=item.get('config_type', "string"),
            config_values=item.get('config_values', None),
            default_value=conf_value,
            is_optional=item.get('is_optional', True),
            description=item.get('description', None),
            priority=priority)

    def get_version_config(self, versions):
        return p.Config(
            name='%s Version' % self._ui_name,
            applicable_target=self.ui_name,
            scope='cluster',
            config_type='dropdown',
            config_values=[(v, v) for v in sorted(versions, reverse=True)],
            is_optional=False,
            description=_LI('Specify the version of the service'),
            priority=1)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            version_eq = self.version == other.version
            name_eq = self.name == other.name
            ui_name_eq = self.ui_name == other.ui_name
            return version_eq and name_eq and ui_name_eq
        return NotImplemented

    def __repr__(self):
        return '<Service %s>' % self.ui_name

    def restart(self, instances):
        for node_process in self.node_processes:
            node_process.restart(instances)

    def home_dir(self, cluster_context):
        args = {
            'mapr_home': cluster_context.mapr_home,
            'name': self.name,
            'version': self.version,
        }
        return '%(mapr_home)s/%(name)s/%(name)s-%(version)s' % args


class Single(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Single, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
