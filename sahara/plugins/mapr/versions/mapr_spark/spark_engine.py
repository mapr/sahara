# Copyright (c) 2015, MapR Technologies
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


import os

from sahara import conductor as c
from sahara import context
from sahara import exceptions as e
from sahara.i18n import _
import sahara.plugins.general.utils as plugin_utils
from sahara.plugins.mapr.services.spark import spark
from sahara.service.edp import job_utils
from sahara.service.edp.spark import engine as base_engine
from sahara.utils import edp

conductor = c.API


class MaprSparkEngine(base_engine.SparkJobEngine):

    SPARK_SUBMIT = '/opt/mapr/spark/spark-1.1.0/bin/spark-submit'

    def run_job(self, job_execution):
        ctx = context.ctx()
        job = conductor.job_get(ctx, job_execution.job_id)

        proxy_configs = job_execution.job_configs.get('proxy_configs')

        # We'll always run the driver program on the master
        master = self.get_random_master()

        # The only requirement is that the dir is writable by the image user

        self.wf_dir = job_utils.create_workflow_dir(master,
                                                    '/tmp/spark-edp',
                                                    job, job_execution.id)
        self.paths = job_utils.upload_job_files(master, self.wf_dir, job,
                                                libs_subdir=False,
                                                proxy_configs=proxy_configs)

        # We can shorten the paths in this case since we'll run out of wf_dir
        paths = [os.path.basename(p) for p in self.paths]

        # jar and we generate paths in order (mains, then libs).
        # When we have a Spark job type, we can require a "main" and set
        # the app jar explicitly to be "main"
        app_jar = paths.pop(0)

        # The rest of the paths will be passed with --jars
        additional_jars = ",".join(paths)
        if additional_jars:
            additional_jars = "--jars " + additional_jars

        # Launch the spark job using spark-submit and deploy_mode = client

        job_class = job_execution.job_configs.configs["edp.java.main_class"]

        args = " ".join(job_execution.job_configs.get('args', []))

        # The redirects of stdout and stderr will preserve output in the wf_dir
        cmd = "sudo -u mapr %s --class %s  --master spark://%s %s %s %s" % (
            self.SPARK_SUBMIT,
            job_class,
            self.get_spark_masters_nodes_with_port(),
            app_jar,
            additional_jars,
            args)

        job_execution = conductor.job_execution_get(ctx, job_execution.id)
        if job_execution.info['status'] == edp.JOB_STATUS_TOBEKILLED:
            return (None, edp.JOB_STATUS_KILLED, None)

        # If an exception is raised here, the job_manager will mark
        # the job failed and log the exception

        with master.remote() as r:
            # Upload the command launch script
            launch = os.path.join(self.wf_dir, "launch_command")
            r.write_file_to(launch, self._job_script())
            r.execute_command("chmod +x %s" % launch)
            ret, stdout = r.execute_command(
                "cd %s; ./launch_command %s > /dev/null 2>&1 & echo $!"
                % (self.wf_dir, cmd))

        if ret == 0:
            # Success, we'll add the wf_dir in job_execution.extra and store
            # pid@instance_id as the job id
            # We know the job is running so return "RUNNING"
            return (stdout.strip() + "@" + master.id,
                    edp.JOB_STATUS_RUNNING,
                    {'spark-path': self.wf_dir})

        # Hmm, no execption but something failed.
        # Since we're using backgrounding with redirect, this is unlikely.
        raise e.EDPError(_("Spark job execution failed. Exit status = "
                           "%(status)s, stdout = %(stdout)s") %
                         {'status': ret, 'stdout': stdout})

    def get_random_master(self):
        masters = plugin_utils.get_instances(self.cluster,
                                             spark.SPARK_MASTER.get_ui_name())
        # return random.choice(masters)
        return masters[0]

    def get_spark_masters_nodes_with_port(self):
        node_list = plugin_utils.get_instances(
            self.cluster, spark.SPARK_MASTER.get_ui_name())
        return ','.join(['%s:%s' % (i.fqdn(), spark.SPARK_MASTER_PORT)
                         for i in node_list])
