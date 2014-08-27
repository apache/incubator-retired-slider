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

java64_home = config['hostLevelParams']['java_home']
hbase_root = config['configurations']['global']['app_root']
hbase_instance_name = config['configurations']['global']['hbase_instance_name']
hbase_user = status_params.hbase_user
user_group = config['configurations']['global']['user_group']

pid_dir = status_params.pid_dir
tmp_dir = config['configurations']['hbase-site']['hbase.tmp.dir']
local_dir = substitute_vars(config['configurations']['hbase-site']['hbase.local.dir'],
                            config['configurations']['hbase-site'])
conf_dir = format("{hbase_root}/conf")
log_dir = config['configurations']['global']['app_log_dir']
input_conf_files_dir = config['configurations']['global']['app_input_conf_dir']

hbase_hdfs_root_dir = config['configurations']['hbase-site']['hbase.rootdir']
rest_port = config['configurations']['global']['hbase_rest_port']
thrift_port = config['configurations']['global']['hbase_thrift_port']
thrift2_port = config['configurations']['global']['hbase_thrift2_port']

master_heapsize = config['configurations']['hbase-env']['hbase_master_heapsize']
regionserver_heapsize = config['configurations']['hbase-env']['hbase_regionserver_heapsize']
regionserver_xmn_max = config['configurations']['hbase-env']['hbase_regionserver_xmn_max']
regionserver_xmn_percent = config['configurations']['hbase-env']['hbase_regionserver_xmn_ratio']
regionserver_xmn_size = calc_xmn_from_xms(regionserver_heapsize, regionserver_xmn_percent, regionserver_xmn_max)

hbase_env_sh_template = config['configurations']['hbase-env']['content']
java_library_path = config['configurations']['global']['java_library_path']
hbase_additional_cp = config['configurations']['global']['hbase_additional_cp']

master_jaas_config_file = default('hbase_master_jaas_config_file', format("{conf_dir}/hbase_master_jaas.conf"))
regionserver_jaas_config_file = default('hbase_regionserver_jaas_config_file',
                                        format("{conf_dir}/hbase_regionserver_jaas.conf"))
master_keytab_path = config['configurations']['hbase-site']['hbase.master.keytab.file']
regionserver_keytab_path = config['configurations']['hbase-site']['hbase.regionserver.keytab.file']

_authentication = config['configurations']['core-site']['hadoop.security.authentication']
security_enabled = ( not is_empty(_authentication) and _authentication == 'kerberos')
if security_enabled:
  _hostname_lowercase = config['hostname'].lower()
  master_jaas_princ = config['configurations']['hbase-site']['hbase.master.kerberos.principal'].replace('_HOST', hostname_lowercase)
  regionserver_jaas_princ = config['configurations']['hbase-site']['hbase.regionserver.kerberos.principal'].replace('_HOST', hostname_lowercase)

kinit_path_local = functions.get_kinit_path(
  [default("kinit_path_local", None), "/usr/bin", "/usr/kerberos/bin", "/usr/sbin"])
if security_enabled:
  kinit_cmd = format("{kinit_path_local} -kt {hbase_user_keytab} {hbase_user};")
else:
  kinit_cmd = ""

# log4j.properties
if (('hbase-log4j' in config['configurations']) and ('content' in config['configurations']['hbase-log4j'])):
  log4j_props = config['configurations']['hbase-log4j']['content']
else:
  log4j_props = None