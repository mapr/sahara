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


class Distro(object):
    def __init__(self, name, install_cmd, version_separator, java_pkg=None):
        self.name = name
        self.install_cmd = install_cmd
        self.version_separator = version_separator
        self.java_pkg = java_pkg

    def get_name(self):
        return self.name

    def get_install_command(self):
        return self.install_cmd

    def get_version_separator(self):
        return self.version_separator

    # FIXME(aosadchyi): Name should be more appropriate
    def create_install_cmd(self, packages):
        v_separator = self.get_version_separator()

        def join_package_version(pv_item):
            p, v = pv_item if len(pv_item) > 1 else (pv_item[0], None)
            return p + v_separator + v + '*' if v else p

        packages = ' '.join(map(join_package_version, packages))
        cmd = '%(install_cmd)s %(packages)s'
        args = {
            'install_cmd': self.get_install_command(),
            'packages': packages,
        }
        return cmd % args

    def add_repo(self, instance, repo):
        pass


UBUNTU = Distro(
    name='Ubuntu',
    install_cmd='apt-get install --force-yes -y',
    version_separator='='
)

CENTOS = Distro(
    name='CentOS',
    install_cmd='yum install -y',
    version_separator='-'
)

RHEL = Distro(
    name='RedHatEnterpriseServer',
    install_cmd='yum install -y',
    version_separator='-'
)

SUSE = Distro(
    name='Suse',
    install_cmd='zypper',
    version_separator=':'
)


def get_all():
    return [
        UBUNTU,
        CENTOS,
        RHEL,
        SUSE
    ]


def get(instance):
    with instance.remote() as r:
        output = r.execute_command('cat /etc/*-release', run_as_root=True)
        name = output[1]
        for d in get_all():
            if d.get_name() in name:
                return d
