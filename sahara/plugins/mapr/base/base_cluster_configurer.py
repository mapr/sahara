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
import os
import uuid

import six

from sahara import conductor
from sahara import context
from sahara.i18n import _LI
from sahara.openstack.common import log as logging
import sahara.plugins.mapr.abstract.configurer as ac
import sahara.plugins.mapr.services.management.management as mng
import sahara.plugins.mapr.services.mapreduce.mapreduce as mr
from sahara.plugins.mapr.services.maprfs import maprfs
from sahara.plugins.mapr.services.mysql import mysql
import sahara.plugins.mapr.services.yarn.yarn as yarn
import sahara.plugins.mapr.util.general as util
from sahara.topology import topology_helper as th
import sahara.utils.configs as sahara_configs
from sahara.utils import files as f


LOG = logging.getLogger(__name__)
conductor = conductor.API

MAPR_REPO_DIR = '/opt/mapr-repository'
_MAPR_HOME = '/opt/mapr'
_JAVA_HOME = '/usr/java/jdk1.7.0_51'
_CONFIGURE_SH_TIMEOUT = 600
_SET_MODE_CMD = 'maprcli cluster mapreduce set -mode '

_TOPO_SCRIPT = 'plugins/mapr/resources/topology.sh'

SERVICE_INSTALL_PRIORITY = [
    mng.Management(),
    yarn.YARNv251(),
    yarn.YARNv241(),
    mr.MapReduce(),
    maprfs.MapRFS(),
]


@six.add_metaclass(abc.ABCMeta)
class BaseConfigurer(ac.AbstractConfigurer):
    def configure(self, cluster_context, instances=None):
        instances = instances or cluster_context.get_instances()

        self._configure_ssh_connection(cluster_context, instances)
        self._install_mapr_repo(cluster_context, instances)
        if not self._is_prebuilt(cluster_context, instances):
            self._prepare_bare_image(cluster_context, instances)
        self._install_services(cluster_context, instances)
        self._configure_topology(cluster_context, instances)
        self._configure_database(cluster_context, instances)
        self._configure_services(cluster_context, instances)
        self.configure_sh_cluster(cluster_context, instances)
        self._set_cluster_mode(cluster_context)
        self._write_config_files(cluster_context, instances)
        self._configure_environment(cluster_context, instances)
        self._update_cluster_info(cluster_context)

    def update(self, cluster_context, instances=None):
        LOG.info(_LI('START: Configuring existing instances.'))
        instances = instances or cluster_context.get_instances()
        existing_insts = [i for i in cluster_context.get_instances()
                          if i.instance_id not in [j.instance_id for j in instances]]
        self._configure_topology(cluster_context, existing_insts)
        if cluster_context.check_for_cldb_or_zookeeper_service(instances):
            self.configure_sh_cluster(cluster_context, existing_insts)
        self._write_config_files(cluster_context, existing_insts)
        self._update_services(cluster_context, existing_insts)
        self._restart_services(cluster_context)
        LOG.info(_LI('END: Configuring existing instances.'))

    def _configure_services(self, cluster_context, instances):
        for service in cluster_context.cluster_services:
            service.configure(cluster_context, instances)

    def _install_services(self, cluster_context, instances):
        for service in self._service_install_sequence(cluster_context):
            service.install(cluster_context, instances)

    def _service_install_sequence(self, cluster_context):
        def key(service):
            if service in SERVICE_INSTALL_PRIORITY:
                return SERVICE_INSTALL_PRIORITY.index(service)
            return -1

        return sorted(cluster_context.cluster_services, key=key, reverse=True)

    def _is_prebuilt(self, cluster_context, instances):
        c_name = cluster_context.cluster.name
        LOG.debug('Checking, if local mapr repo exists on cluster=%s', c_name)
        with cluster_context.some_instance.remote() as r:
            ec, out = r.execute_command(
                cmd='[ -d %s ]' % MAPR_REPO_DIR,
                run_as_root=True,
                timeout=1800,
                raise_when_error=False
            )
            return ec == 0

    def _prepare_bare_image(self, cluster_ctx, instances):
        def get_install_java_cmd():
            return f.get_file_text('plugins/mapr/resources/install_java.sh')

        def get_install_scala_cmd():
            return f.get_file_text('plugins/mapr/resources/install_scala.sh')

        def get_install_mysql_client_cmd():
            file_name = 'install_mysql_client.sh'
            return f.get_file_text('plugins/mapr/resources/%s' % file_name)

        def install_script(instance, **kwargs):
            content = kwargs.get('content', None)
            distro = kwargs.get('distro', None)
            path = '/tmp/%s.sh' % uuid.uuid4()
            args = {
                'path': path,
                'distro': distro
            }
            cmd = ('chmod +x %(path)s && '
                   'bash %(path)s %(distro)s') % args
            with instance.remote() as r:
                r.write_file_to(path, content)
                r.execute_command(cmd, run_as_root=True,
                                  timeout=3000)

        LOG.debug('START: Preparing bare images on cluster=%s' %
                  cluster_ctx.cluster.name)

        distro = cluster_ctx.distro.name

        LOG.debug('Start installing Java')
        self._execute_on_instances(install_script, cluster_ctx, instances,
                                   distro=distro,
                                   content=get_install_java_cmd())

        LOG.debug('Start installing Scala')
        self._execute_on_instances(install_script, cluster_ctx, instances,
                                   distro=distro,
                                   content=get_install_scala_cmd())

        LOG.debug('Start installing MySQL client')
        self._execute_on_instances(install_script, cluster_ctx, instances,
                                   distro=distro,
                                   content=get_install_mysql_client_cmd())

        LOG.debug('END: Preparing bare images on cluster=%s' %
                  cluster_ctx.cluster.name)

    def _configure_topology(self, context, instances):
        node_aware = context.is_node_aware
        LOG.info(_LI('START: configuring topology data.'))
        if node_aware:
            LOG.debug('Data locality is enabled.')
            LOG.debug('Generating topology map.')

            topo_map = th.generate_topology_map(context.cluster, node_aware)
            topo_str = '\n'.join(['%s %s' % (h, r)
                                  for h, r in six.iteritems(topo_map)])
            topology_data = '%s/topology.data' % _MAPR_HOME
            topology_script = '%s/topology.sh' % _MAPR_HOME
            files = {
                topology_data: topo_str,
                topology_script: f.get_file_text(_TOPO_SCRIPT),
            }
            chmod_cmd = 'chmod +x %s' % topology_script

            for instance in instances:
                with instance.remote() as r:
                    r.write_files_to(files, run_as_root=True)
                    r.execute_command(chmod_cmd, run_as_root=True)
        else:
            LOG.debug('Data locality is disabled.')
        LOG.info(_LI('END: configuring topology data.'))

    def _execute_on_instances(self, function, cluster_context, instances,
                              **kwargs):
        LOG.debug('Executing %s on cluster=%s' % (
                  function.__name__, cluster_context.cluster.name))
        with context.ThreadGroup() as tg:
            for instance in instances:
                tg.spawn('%s-execution' % function.__name__,
                         function, instance, **kwargs)

    def _write_config_files(self, cluster_context, instances):
        LOG.info(_LI('START: writing config files'))

        def get_node_groups(instances):
            return util.unique_list(instances, lambda i: i.node_group)

        for ng in get_node_groups(instances):
            ng_services = cluster_context.get_cluster_services(ng)
            ng_user_configs = ng.configuration()
            ng_default_configs = cluster_context.get_services_configs_dict(
                ng_services)
            ng_configs = sahara_configs.merge_configs(
                ng_default_configs, ng_user_configs)
            ng_config_files = dict()
            for service in ng_services:
                service_conf_files = service.get_config_files(
                    cluster_context=cluster_context,
                    configs=ng_configs[service.ui_name],
                    instance=ng.instances[0]
                )
                for conf_file in service_conf_files:
                    LOG.info('Config file %s: name %s; path %s' % (
                        conf_file, conf_file.f_name, conf_file.remote_path))
                    ng_config_files.update({
                        conf_file.remote_path: conf_file.render()
                    })
            current_ng_instances = filter(
                lambda i: i in instances, ng.instances)
            self._write_ng_config_files(current_ng_instances, ng_config_files)
        LOG.info(_LI('END: writing config files'))

    def _write_ng_config_files(self, instances, conf_files):
        with context.ThreadGroup() as tg:
            for instance in instances:
                tg.spawn('write-config-files-%s' % instance.id,
                         self._write_config_files_instance, instance,
                         conf_files)

    def _configure_environment(self, cluster_context, instances):
        self.configure_general_environment(cluster_context, instances)
        self._post_install_services(cluster_context, instances)

    def _configure_database(self, cluster_context, instances):
        mysql_instance = mysql.MySQL.get_db_instance(cluster_context)
        distro_name = cluster_context.distro.name
        mysql.MySQL.install_mysql(mysql_instance, distro_name)
        mysql.MySQL.start_mysql_server(cluster_context)
        mysql.MySQL.create_databases(cluster_context, instances)

    @staticmethod
    def _write_config_files_instance(instance, config_files):
        paths = six.iterkeys(config_files)
        with instance.remote() as r:
            for path in paths:
                r.execute_command('mkdir -p ' + os.path.dirname(path),
                                  run_as_root=True)
            r.write_files_to(config_files, run_as_root=True)

    def _post_install_services(self, cluster_context, instances):
        LOG.info(_LI('START: Services post install configuration'))
        for s in cluster_context.cluster_services:
            s.post_install(cluster_context, instances)
        LOG.info(_LI('END: Services post install configuration'))

    def _update_cluster_info(self, cluster_context):
        LOG.info(_LI('START: updating cluster information.'))
        info = dict()
        for service in cluster_context.cluster_services:
            for uri_info in service.ui_info:
                title, process, url = uri_info
                info.update({
                    title: {
                        'WebUI': url % cluster_context.get_instance_ip(process)
                    }
                })

        ctx = context.ctx()
        conductor.cluster_update(ctx, cluster_context.cluster, {'info': info})
        LOG.info(_LI('END: updating cluster information.'))

    def configure_general_environment(self, cluster_context, instances=None):
        LOG.info(_LI('START: Post configuration on cluster=%s'),
                 cluster_context.cluster.name)

        if not instances:
            instances = cluster_context.get_instances()

        def set_user_password(instance):
            LOG.debug('Setting MapR user password')
            if self.mapr_user_exists(instance):
                with instance.remote() as r:
                    r.execute_command(
                        'echo "%s:%s"|chpasswd' % ('mapr', 'mapr'),
                        run_as_root=True)
            else:
                LOG.debug('Skip: MapR user does not exists')

        def create_home_mapr(instance):
            target_path = '/home/mapr'
            LOG.debug("Creating %s directory" % target_path)
            args = {'path': target_path}
            cmd = 'mkdir -p %(path)s && chown mapr:mapr %(path)s' % args
            if self.mapr_user_exists(instance):
                with instance.remote() as r:
                    r.execute_command(cmd, run_as_root=True)
            else:
                LOG.debug('Skip: MapR user does not exists')

        self._execute_on_instances(set_user_password, cluster_context,
                                   instances)
        self._execute_on_instances(create_home_mapr, cluster_context,
                                   instances)

    def configure_sh_cluster(self, cluster_context, instances):
        LOG.info(_LI('START: Executing configure.sh on cluster=%s'),
                 cluster_context.cluster.name)

        if not instances:
            instances = cluster_context.get_instances()
        script = cluster_context.configure_sh

        db_specs = dict(mysql.MySQL.METRICS_SPECS._asdict())
        db_specs.update(
            {'host': mysql.MySQL.get_db_instance(cluster_context).fqdn(),
             'port': mysql.MySQL.MYSQL_SERVER_PORT})

        with context.ThreadGroup() as tg:
            for instance in instances:
                tg.spawn('configure-sh-%s' % instance.id,
                         self._configure_sh_instance, cluster_context,
                         instance, script, db_specs)
        LOG.info(_LI('END: Executing configure.sh on cluster=%s'),
                 cluster_context.cluster.name)

    def _configure_sh_instance(self, context, instance, command, specs):
        LOG.info(_LI('START: Executing configure.sh on instance=%s'),
                 instance.management_ip)

        if not self.mapr_user_exists(instance):
            command += ' --create-user'

        if context.check_for_process(instance, mng.METRICS):
            command += (' -d %(host)s:%(port)s -du %(user)s -dp %(password)s '
                        '-ds %(db_name)s') % specs

        with instance.remote() as r:
            r.execute_command('sudo -i ' + command,
                              timeout=_CONFIGURE_SH_TIMEOUT)

        LOG.info(_LI('END: Executing configure.sh on instance=%s'),
                 instance.management_ip)

    def _configure_ssh_connection(self, cluster_context, instances):
        def keep_alive_connection(instance):
            echo_param = 'echo "KeepAlive yes" >> ~/.ssh/config'
            echo_timeout = 'echo "ServerAliveInterval 60" >> ~/.ssh/config'
            with instance.remote() as r:
                r.execute_command(echo_param)
                r.execute_command(echo_timeout)

        self._execute_on_instances(keep_alive_connection,
                                   cluster_context, instances)

    def mapr_user_exists(self, instance):
            with instance.remote() as r:
                ec, out = r.execute_command(
                    'id -u mapr', run_as_root=True, raise_when_error=False)
            return ec == 0

    def post_start(self, c_context, instances=None):
        instances = instances or c_context.get_instances()
        LOG.info(_LI('START: Services post start configuration'))
        for service in c_context.cluster_services:
            updated = c_context.filter_instances(instances, service=service)
            service.post_start(c_context, updated)
        LOG.info(_LI('END: Services post start configuration'))

    def _set_cluster_mode(self, cluster_context):
        cluster_mode = cluster_context.cluster_mode
        if not cluster_mode:
            return
        cldb = cluster_context.get_instance(maprfs.CLDB)
        with cldb.remote() as r:
            cmd = 'sudo -u mapr maprcli cluster mapreduce set -mode %s'
            r.execute_command(cmd % cluster_mode)

    def _install_mapr_repo(self, cluster_context, instances):
        def add_repo(instance, **kwargs):
            with instance.remote() as r:
                script = '/tmp/repo_install.sh'
                data = cluster_context.get_install_repo_script_data()
                r.write_file_to(script, data, run_as_root=True)
                r.execute_command('chmod +x %s' % script, run_as_root=True)
                r.execute_command('%s %s' % (script, kwargs.get('distro')),
                                  run_as_root=True, raise_when_error=False)

        d_name = cluster_context.distro.get_name()
        self._execute_on_instances(
            add_repo, cluster_context, instances, distro=d_name)

    def _update_services(self, c_context, instances):
        for service in c_context.cluster_services:
            updated = c_context.filter_instances(instances, service=service)
            service.update(c_context, updated)

    def _restart_services(self, cluster_context):
        restart = cluster_context.should_be_restarted
        for service, instances in six.iteritems(restart):
            service.restart(util.unique_list(instances))
