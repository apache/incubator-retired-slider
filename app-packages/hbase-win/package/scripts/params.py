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
app_version = config['configurations']['global']['app_version']
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

"""
Read various ports
"""
rest_port = default("/configurations/global/hbase_rest_port", 1700)
thrift_port = default("/configurations/global/hbase_thrift_port", 9090)
thrift2_port = default("/configurations/global/hbase_thrift2_port", 9091)
thrift_info_port = default("/configurations/global/hbase_info_thrift_port", 9095)
thrift2_info_port = default("/configurations/global/hbase_info_thrift2_port", 9096)

"""
Compute or read various heap sizes
"""
master_heapsize = config['configurations']['hbase-env']['hbase_master_heapsize']
regionserver_heapsize = config['configurations']['hbase-env']['hbase_regionserver_heapsize']
regionserver_xmn_max = config['configurations']['hbase-env']['hbase_regionserver_xmn_max']
regionserver_xmn_percent = config['configurations']['hbase-env']['hbase_regionserver_xmn_ratio']
regionserver_xmn_size = calc_xmn_from_xms(regionserver_heapsize, regionserver_xmn_percent, regionserver_xmn_max)

restserver_heapsize =  default("/configurations/hbase-env/hbase_restserver_heapsize", "512m")
thriftserver_heapsize =  default("/configurations/hbase-env/hbase_thriftserver_heapsize", "512m")
thrift2server_heapsize =  default("/configurations/hbase-env/hbase_thrift2server_heapsize", "512m")

hbase_env_sh_template = config['configurations']['hbase-env']['content']
java_library_path = config['configurations']['global']['java_library_path']
hbase_additional_cp = config['configurations']['global']['hbase_additional_cp']

# log4j.properties
if (('hbase-log4j' in config['configurations']) and ('content' in config['configurations']['hbase-log4j'])):
  log4j_props = config['configurations']['hbase-log4j']['content']
else:
  log4j_props = None