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


import abc
import collections as c

import six

from sahara.i18n import _LI
from sahara.openstack.common import log as logging
import sahara.plugins.general.utils as u
import sahara.plugins.mapr.abstract.version_handler as vh
import sahara.plugins.mapr.base.base_cluster_configurer as base_conf
import sahara.plugins.mapr.base.base_cluster_validator as bv
import sahara.plugins.mapr.base.base_edp_engine as edp
import sahara.plugins.mapr.base.base_node_manager as bs
import sahara.plugins.mapr.util.general as util


LOG = logging.getLogger(__name__)


class BaseVersionHandler(vh.AbstractVersionHandler):
    def __init__(self):
        self.validator = bv.BaseValidator()
        self.configurer = base_conf.BaseConfigurer()
        self.node_manager = bs.BaseNodeManager()
        self._version = None
        self._required_services = []
        self._services = []

    def get_plugin_version(self):
        return self._version

    def get_edp_engine(self, cluster, job_type):
        if job_type in edp.MapROozieJobEngine.get_supported_job_types():
            return edp.MapROozieJobEngine(cluster)
        return None

    @abc.abstractmethod
    def get_context(self, cluster, added=None, removed=None):
        return

    def get_services(self):
        return self._services

    def get_required_services(self):
        return self._required_services

    def get_np_dict(self):
        np_dict = c.defaultdict(set)
        result = dict()
        for service in self.get_services():
            for np in service.node_processes:
                np_dict[service.ui_name].add(np.ui_name)
        for k, v in six.iteritems(np_dict):
            result.update({k: list(v)})
        return result

    def get_configs(self):
        configs = [c for s in self.get_services() for c in s.get_configs()]
        result = util.unique_list(configs)
        result += self._get_version_configs()
        return result

    def _get_version_configs(self):
        services = self.get_services()

        service_version_dict = c.defaultdict(list)
        for service in services:
            service_version_dict[service.ui_name].append(service.version)

        result = []
        for service in services:
            versions = service_version_dict[service.ui_name]
            if len(versions) > 1:
                result.append(service.get_version_config(versions))

        return result

    def get_configs_dict(self):
        configs = dict()
        for service in self.get_services():
            configs.update(service.get_configs_dict())
        return configs

    def configure_cluster(self, cluster):
        instances = u.get_instances(cluster)
        cluster_context = self.get_context(cluster, added=instances)
        self.configurer.configure(cluster_context)

    def start_cluster(self, cluster):
        instances = u.get_instances(cluster)
        cluster_context = self.get_context(cluster, added=instances)
        self.node_manager.start(cluster_context)
        self.configurer.post_start(cluster_context)

    def validate(self, cluster):
        cluster_context = self.get_context(cluster)
        self.validator.validate(cluster_context)

    def validate_scaling(self, cluster, existing, additional):
        cluster_context = self.get_context(cluster)
        self.validator.validate_scaling(cluster_context, existing, additional)

    def scale_cluster(self, cluster, instances):
        LOG.info(_LI('START: Cluster scaling. Cluster = %s'), cluster.name)
        cluster_context = self.get_context(cluster, added=instances)
        cluster_context._cluster_services = None
        self.configurer.configure(cluster_context, instances)
        self.configurer.update(cluster_context, instances)
        self.node_manager.start(cluster_context, instances)
        LOG.info(_LI('END: Cluster scaling. Cluster = %s'), cluster.name)

    def decommission_nodes(self, cluster, instances):
        LOG.info(_LI('START: Decommission'))
        cluster_context = self.get_context(cluster, removed=instances)
        cluster_context._cluster_services = None
        self.node_manager.move_nodes(cluster_context, instances)
        self.node_manager.stop(cluster_context, instances)
        self.node_manager.await_no_heartbeat()
        self.node_manager.remove_nodes(cluster_context, instances)
        self.configurer.update(cluster_context, instances)
        LOG.info(_LI('END: Decommission'))

    def get_open_ports(self, node_group):
        result = []
        for service in self.get_services():
            for node_process in service.get_node_processes():
                if node_process.get_ui_name() in node_group.node_processes:
                    result += node_process.get_open_ports()
        return util.unique_list(result)
