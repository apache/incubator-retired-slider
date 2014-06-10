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

# user and status
accumulo_user = status_params.accumulo_user
user_group = config['configurations']['global']['user_group']
pid_dir = status_params.pid_dir

# accumulo env
java64_home = config['hostLevelParams']['java_home']
hadoop_prefix = config['configurations']['global']['hadoop_prefix']
hadoop_conf_dir = config['configurations']['global']['hadoop_conf_dir']
zookeeper_home = config['configurations']['global']['zookeeper_home']
master_heapsize = config['configurations']['global']['master_heapsize']
tserver_heapsize = config['configurations']['global']['tserver_heapsize']
monitor_heapsize = config['configurations']['global']['monitor_heapsize']
gc_heapsize = config['configurations']['global']['gc_heapsize']
other_heapsize = config['configurations']['global']['other_heapsize']

# accumulo local directory structure
accumulo_root = config['configurations']['global']['app_root']
conf_dir = None
if ('accumulo_conf_dir' in config['configurations']['global']):
  conf_dir = config['configurations']['global']['accumulo_conf_dir']
else:
  conf_dir = format("{accumulo_root}/conf")
log_dir = config['configurations']['global']['app_log_dir']
daemon_script = format("{accumulo_root}/bin/accumulo")

# accumulo monitor certificate properties
monitor_security_enabled = config['configurations']['global']['monitor_protocol'] == "https"
keystore_path = format("{accumulo_root}/conf/keystore.jks")
truststore_path = format("{accumulo_root}/conf/cacerts.jks")
cert_path = format("{accumulo_root}/conf/server.cer")
keystore_property = "monitor.ssl.keyStore"
keystore_password_property = "monitor.ssl.keyStorePassword"
truststore_property = "monitor.ssl.trustStore"
truststore_password_property = "monitor.ssl.trustStorePassword"

# accumulo initialization parameters
accumulo_instance_name = config['configurations']['global']['accumulo_instance_name']
accumulo_root_password = config['configurations']['global']['accumulo_root_password']
accumulo_hdfs_root_dir = config['configurations']['accumulo-site']['instance.dfs.dir']

#log4j.properties
if (('accumulo-log4j' in config['configurations']) and ('content' in config['configurations']['accumulo-log4j'])):
  log4j_props = config['configurations']['accumulo-log4j']['content']
else:
  log4j_props = None
