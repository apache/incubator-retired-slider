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

from functions import calc_xmn_from_xms
from resource_management import *
import status_params

# server configurations
config = Script.get_config()

hbase_root = config['configurations']['global']['app_root']
app_version = config['configurations']['global']['app_version']
app_name = config['clusterName']
conf_dir = format("{hbase_root}/conf")
daemon_script = format("{hbase_root}/bin/hbase-daemon.sh")

hbase_user = status_params.hbase_user
_authentication = config['configurations']['core-site']['hadoop.security.authentication']
security_enabled = ( not is_empty(_authentication) and _authentication == 'kerberos')
user_group = config['configurations']['global']['user_group']

# this is "hadoop-metrics.properties" for 1.x stacks
metric_prop_file_name = "hadoop-metrics2-hbase.properties"

# not supporting 32 bit jdk.
java64_home = config['hostLevelParams']['java_home']

log_dir = config['configurations']['global']['app_log_dir']
#configuration for HBASE_OPTS
hbase_opts = default('/configurations/hbase-env/hbase_opts', '')
master_heapsize = config['configurations']['hbase-env']['hbase_master_heapsize']

regionserver_heapsize = config['configurations']['hbase-env']['hbase_regionserver_heapsize']
regionserver_xmn_max = config['configurations']['hbase-env']['hbase_regionserver_xmn_max']
regionserver_xmn_percent = config['configurations']['hbase-env']['hbase_regionserver_xmn_ratio']
regionserver_xmn_size = calc_xmn_from_xms(regionserver_heapsize, regionserver_xmn_percent, regionserver_xmn_max)

pid_dir = status_params.pid_dir
tmp_dir = config['configurations']['hbase-site']['hbase.tmp.dir']
local_dir = substitute_vars(config['configurations']['hbase-site']['hbase.local.dir'], config['configurations']['hbase-site'])
input_conf_files_dir = config['configurations']['global']['app_input_conf_dir']

client_jaas_config_file = default('hbase_client_jaas_config_file', format("{conf_dir}/hbase_client_jaas.conf"))
master_jaas_config_file = default('hbase_master_jaas_config_file', format("{conf_dir}/hbase_master_jaas.conf"))
regionserver_jaas_config_file = default('hbase_regionserver_jaas_config_file', format("{conf_dir}/hbase_regionserver_jaas.conf"))

metric_collector_host = default('/configurations/global/metric_collector_host', '')
metric_collector_port = default('/configurations/global/metric_collector_port', '')
metric_collector_lib = default('/configurations/global/metric_collector_lib', '')
has_metric_collector = 1
if not metric_collector_lib:
  has_metric_collector = 0

rest_port = config['configurations']['global']['hbase_rest_port']
rest_infoport = default('/configurations/global/hbase_rest_infoport', '')
rest_readonly = default('/configurations/global/hbase_rest_readonly', '')

thrift_port = default('/configurations/global/hbase_thrift_port', '')
thrift_keepalive_sec = default('/configurations/global/hbase_thrift_keepalive_sec', '')
thrift_infoport = default('/configurations/global/hbase_thrift_infoport', '')
thrift_nonblocking = default('/configurations/global/hbase_thrift_nonblocking', '')
thrift_minWorkers = default('/configurations/global/hbase_thrift_minWorkers', '')
thrift_queue = default('/configurations/global/hbase_thrift_queue', '')
thrift_workers = default('/configurations/global/hbase_thrift_workers', '')
thrift_compact = default('/configurations/global/hbase_thrift_compact', '')
thrift_framed = default('/configurations/global/hbase_thrift_framed', '')
thrift2_port = default('/configurations/global/hbase_thrift2_port', '')
thrift2_compact = default('/configurations/global/hbase_thrift2_compact', '')
thrift2_framed = default('/configurations/global/hbase_thrift2_framed', '')
thrift2_infoport = default('/configurations/global/hbase_thrift2_infoport', '')
thrift2_nonblocking = default('/configurations/global/hbase_thrift2_nonblocking', '')

if security_enabled:
  _hostname_lowercase = config['hostname'].lower()
  master_jaas_princ = config['configurations']['hbase-site']['hbase.master.kerberos.principal'].replace('_HOST',_hostname_lowercase)
  regionserver_jaas_princ = config['configurations']['hbase-site']['hbase.regionserver.kerberos.principal'].replace('_HOST',_hostname_lowercase)


master_keytab_path = config['configurations']['hbase-site']['hbase.master.keytab.file']
regionserver_keytab_path = config['configurations']['hbase-site']['hbase.regionserver.keytab.file']
kinit_path_local = functions.get_kinit_path([default("kinit_path_local",None), "/usr/bin", "/usr/kerberos/bin", "/usr/sbin"])

#log4j.properties
if (('hbase-log4j' in config['configurations']) and ('content' in config['configurations']['hbase-log4j'])):
  log4j_props = config['configurations']['hbase-log4j']['content']
else:
  log4j_props = None

hbase_env_sh_template = config['configurations']['hbase-env']['content']

hbase_hdfs_root_dir = config['configurations']['hbase-site']['hbase.rootdir']
hbase_staging_dir = config['configurations']['hbase-site']['hbase.stagingdir']
#for create_hdfs_directory
hostname = config["hostname"]
hadoop_conf_dir = "/etc/hadoop/conf"
hdfs_user_keytab = config['configurations']['global']['hdfs_user_keytab']
hdfs_user = config['configurations']['global']['hdfs_user']
kinit_path_local = functions.get_kinit_path([default("kinit_path_local",None), "/usr/bin", "/usr/kerberos/bin", "/usr/sbin"])
import functools
#create partial functions with common arguments for every HdfsDirectory call
#to create hdfs directory we need to call params.HdfsDirectory in code
HdfsDirectory = functools.partial(
  HdfsDirectory,
  conf_dir=hadoop_conf_dir,
  hdfs_user=hdfs_user,
  security_enabled = security_enabled,
  keytab = hdfs_user_keytab,
  kinit_path_local = kinit_path_local
)
