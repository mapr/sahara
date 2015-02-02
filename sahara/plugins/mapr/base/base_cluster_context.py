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


import collections
import logging

from oslo.config import cfg

import sahara.plugins.general.utils as u
import sahara.plugins.mapr.abstract.cluster_context as cc
import sahara.plugins.mapr.domain.distro as distro
import sahara.plugins.mapr.domain.node_process as np
import sahara.plugins.mapr.services.management.management as mng
import sahara.plugins.mapr.services.maprfs.maprfs as mfs
import sahara.plugins.mapr.services.oozie.oozie as oozie
import sahara.plugins.mapr.services.spark.spark as spark
from sahara.plugins.mapr.services.swift import swift
import sahara.plugins.mapr.services.yarn.yarn as yarn
import sahara.plugins.mapr.util.general as g


CONF = cfg.CONF
CONF.import_opt("enable_data_locality", "sahara.topology.topology_helper")

LOG = logging.getLogger(__name__)


def _get_node_process_name(node_process):
    name = None
    if isinstance(node_process, np.NodeProcess):
        name = node_process.ui_name
    elif isinstance(node_process, basestring):
        name = node_process
    return name


class BaseClusterContext(cc.AbstractClusterContext):
    def __init__(self, cluster, version_handler, added=None, removed=None):
        self._cluster = cluster
        self._distro = None
        self.all_services_list = version_handler.get_services()
        self._required_services = version_handler.get_required_services()
        self._cluster_services = None
        self._mapr_home = '/opt/mapr'
        self._name_node_uri = 'maprfs:///'
        self._cluster_mode = None
        self._node_aware = None
        self._oozie_server_uri = None
        self._oozie_server = None
        self._oozie_http = None
        self._some_instance = None
        self._configure_sh = None
        self._mapr_db = None
        self._hadoop_version = None
        self._added_instances = added or []
        self._removed_instances = removed or []
        self._changed_instances = (
            self._added_instances + self._removed_instances)
        self._restart = collections.defaultdict(list)

    @property
    def cluster(self):
        return self._cluster

    @property
    def cluster_services(self):
        if not self._cluster_services:
            self._cluster_services = self.get_cluster_services()
        return self._cluster_services

    @property
    def required_services(self):
        return self._required_services

    @property
    def mapr_home(self):
        return self._mapr_home

    @property
    def hadoop_version(self):
        return self._hadoop_version

    @property
    def hadoop_home(self):
        f = '%(mapr_home)s/hadoop/hadoop-%(hadoop_version)s'
        args = {
            'mapr_home': self.mapr_home,
            'hadoop_version': self.hadoop_version,
        }
        return f % args

    @property
    def name_node_uri(self):
        return self._name_node_uri

    @property
    def oozie_server_uri(self):
        if not self._oozie_server_uri:
            oozie_http = self.oozie_http
            url = 'http://%s/oozie' % oozie_http if oozie_http else None
            self._oozie_server_uri = url
        return self._oozie_server_uri

    @property
    def oozie_server(self):
        if not self._oozie_server:
            self._oozie_server = self.get_instance(oozie.OOZIE)
        return self._oozie_server

    @property
    def oozie_http(self):
        if not self._oozie_http:
            oozie_server = self.oozie_server
            ip = oozie_server.management_ip if oozie_server else None
            self._oozie_http = '%s:11000' % ip if ip else None
        return self._oozie_http

    @property
    def cluster_mode(self):
        return self._cluster_mode

    @property
    def is_node_aware(self):
        return self._node_aware and CONF.enable_data_locality

    @property
    def some_instance(self):
        if not self._some_instance:
            self._some_instance = self.cluster.node_groups[0].instances[0]
        return self._some_instance

    @property
    def distro(self):
        if not self._distro:
            self._distro = distro.get(self.some_instance)
        return self._distro

    @property
    def mapr_db(self):
        if self._mapr_db is None:
            mapr_db = mfs.MapRFS.ENABLE_MAPR_DB_CONFIG
            mapr_db = self._get_cluster_config_value(mapr_db)
            self._mapr_db = '-noDB' if not mapr_db else ''
        return self._mapr_db

    @property
    def configure_sh(self):
        if not self._configure_sh:
            f = ('%(script_path)s'
                 ' -N %(cluster_name)s'
                 ' -C %(cldbs)s'
                 ' -Z %(zookeepers)s'
                 ' -no-autostart -f %(m7)s')
            args = {
                'script_path': '/opt/mapr/server/configure.sh',
                'cluster_name': self.cluster.name,
                'cldbs': self.get_cldb_nodes_ip(),
                'zookeepers': self.get_zookeeper_nodes_ip(),
                'm7': self.mapr_db
            }
            self._configure_sh = f % args
        return self._configure_sh

    def _get_cluster_config_value(self, config):
        cluster_configs = self.cluster.cluster_configs
        service = config.applicable_target
        name = config.name
        if service in cluster_configs and name in cluster_configs[service]:
            return cluster_configs[service][name]
        else:
            return config.default_value

    def get_instances(self, node_process=None):
        name = _get_node_process_name(node_process)
        return u.get_instances(self.cluster, name)

    def get_instance(self, node_process):
        name = _get_node_process_name(node_process)
        i = u.get_instances(self.cluster, name)
        return i[0] if i else None

    def get_instances_ip(self, node_process):
        return [i.management_ip for i in self.get_instances(node_process)]

    def get_instance_ip(self, node_process):
        i = self.get_instance(node_process)
        return i.management_ip if i else None

    def get_zookeeper_nodes_ip_with_port(self, separator=','):
        return separator.join(['%s:%s' % (ip, mng.ZK_CLIENT_PORT)
                               for ip in self.get_instances_ip(mng.ZOOKEEPER)])

    def check_for_process(self, instance, process):
        processes = instance.node_group.node_processes
        name = _get_node_process_name(process)
        return name in processes

    def get_services_configs_dict(self, services=None):
        if not services:
            services = self.cluster_services
        result = dict()
        for service in services:
            result.update(service.get_configs_dict())
        return result

    def get_configure_sh_path(self):
        return '/opt/mapr/server/configure.sh'

    def get_chosen_service_version(self, service_name):
        service_configs = self.cluster.cluster_configs.get(service_name, None)
        if not service_configs:
            return None
        return service_configs.get('%s Version' % service_name, None)

    def get_cluster_services(self, node_group=None):
        node_processes = None

        if node_group:
            node_processes = node_group.node_processes
        else:
            node_processes = [np for ng in self.cluster.node_groups
                              for np in ng.node_processes]
            node_processes = g.unique_list(node_processes)
        services = g.unique_list(node_processes, self.get_service)

        return services + [swift.Swift()]

    def get_service(self, node_process):
        ui_name = self.get_service_name_by_node_process(node_process)
        if ui_name is None:
            raise ValueError('Service not found in services list')
        version = self.get_chosen_service_version(ui_name)
        service = self._find_service_instance(ui_name, version)
        if service is None:
            raise ValueError('Can not map service')
        return service

    def _find_service_instance(self, ui_name, version):
        for service in self.all_services_list:
            if service.ui_name == ui_name:
                if version is not None and service.version != version:
                    continue
                return service

    def get_service_name_by_node_process(self, node_process):
        node_process = _get_node_process_name(node_process)
        for service in self.all_services_list:
            node_processes = [np.ui_name for np in service.node_processes]
            if node_process in node_processes:
                return service.ui_name

    def get_instances_count(self, node_process=None):
        name = _get_node_process_name(node_process)
        return u.get_instances_count(self.cluster, name)

    def get_node_groups(self, node_process=None):
        name = _get_node_process_name(node_process)
        return u.get_node_groups(self.cluster, name)

    def get_cldb_nodes_ip(self, separator=','):
        return separator.join(self.get_instances_ip(mfs.CLDB))

    def get_zookeeper_nodes_ip(self, separator=','):
        return separator.join(
            self.get_instances_ip(mng.ZOOKEEPER))

    def get_spark_slaves_hosts(self):
        spark_instances = self.get_instances(spark.SPARK_SLAVE)
        return [inst.fqdn() for inst in spark_instances]

    def get_resourcemanager_ip(self):
        return self.get_instance_ip(yarn.RESOURCE_MANAGER)

    def get_historyserver_ip(self):
        return self.get_instance_ip(yarn.HISTORY_SERVER)

    def check_for_cldb_or_zookeeper_service(self, instances):
        for inst in instances:
            zookeepers = self.check_for_process(inst, mng.ZOOKEEPER)
            cldbs = self.check_for_process(inst, mfs.CLDB)
            if zookeepers or cldbs:
                return True
        return False

    def is_present(self, service):
        return service in self.cluster_services

    def filter_instances(self, instances, node_process=None, service=None):
        if node_process:
            has_np = lambda i: self.check_for_process(i, node_process)
            return filter(has_np, instances)
        if service:
            result = []
            for instance in instances:
                for node_process in service.node_processes:
                    if self.check_for_process(instance, node_process):
                        result += [instance]
                        break
            return result
        return list(instances)

    def removed_instances(self, node_process=None, service=None):
        instances = self._removed_instances
        return self.filter_instances(instances, node_process, service)

    def added_instances(self, node_process=None, service=None):
        instances = self._added_instances
        return self.filter_instances(instances, node_process, service)

    def changed_instances(self, node_process=None, service=None):
        instances = self._changed_instances
        return self.filter_instances(instances, node_process, service)

    @property
    def should_be_restarted(self):
        return self._restart
