#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from resource_management import *
import status_params

# server configurations
config = Script.get_config()
hostname = config["public_hostname"]

# user and status
accumulo_user = status_params.accumulo_user
user_group = config['configurations']['global']['user_group']
pid_dir = status_params.pid_dir

# accumulo env
java64_home = config['hostLevelParams']['java_home']
hadoop_prefix = config['configurations']['accumulo-env']['hadoop_prefix']
hadoop_conf_dir = config['configurations']['accumulo-env']['hadoop_conf_dir']
zookeeper_home = config['configurations']['accumulo-env']['zookeeper_home']
zookeeper_host = config['configurations']['accumulo-site']['instance.zookeeper.host']
master_heapsize = config['configurations']['accumulo-env']['master_heapsize']
tserver_heapsize = config['configurations']['accumulo-env']['tserver_heapsize']
monitor_heapsize = config['configurations']['accumulo-env']['monitor_heapsize']
gc_heapsize = config['configurations']['accumulo-env']['gc_heapsize']
other_heapsize = config['configurations']['accumulo-env']['other_heapsize']
env_sh_template = config['configurations']['accumulo-env']['server_content']

# accumulo local directory structure
accumulo_root = config['configurations']['global']['app_root']
app_version = config['configurations']['global']['app_version']
app_name = config['clusterName']
conf_dir = format("{accumulo_root}/conf")
log_dir = config['configurations']['global']['app_log_dir']
daemon_script = format("{accumulo_root}/bin/accumulo")
proxy_conf = format("{conf_dir}/proxy.properties")

# accumulo kerberos user auth
kerberos_auth_enabled = False
if 'instance.security.authenticator' in config['configurations']['accumulo-site']\
    and "org.apache.accumulo.server.security.handler.KerberosAuthenticator" == config['configurations']['accumulo-site']['instance.security.authenticator']:
  kerberos_auth_enabled = True

# accumulo initialization parameters
accumulo_instance_name = config['configurations']['client']['instance.name']
accumulo_root_principal = config['configurations']['global']['accumulo_root_principal']
accumulo_root_password = config['configurations']['global']['accumulo_root_password']
accumulo_hdfs_root_dir = config['configurations']['accumulo-site']['instance.volumes'].split(",")[0]

#log4j.properties
if (('accumulo-log4j' in config['configurations']) and ('content' in config['configurations']['accumulo-log4j'])):
  log4j_props = config['configurations']['accumulo-log4j']['content']
else:
  log4j_props = None

metric_collector_host = default('/configurations/global/metric_collector_host', '')
metric_collector_port = default('/configurations/global/metric_collector_port', '')
metric_collector_lib = default('/configurations/global/metric_collector_lib', '')
has_metric_collector = 1
if not metric_collector_lib:
  has_metric_collector = 0

