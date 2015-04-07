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

app_root = config['configurations']['global']['app_root']
app_version = config['configurations']['global']['app_version']
app_name = config['clusterName']
conf_dir = format("{app_root}/conf")
storm_user = config['configurations']['global']['app_user']
log_dir = config['configurations']['global']['app_log_dir']
pid_dir = status_params.pid_dir
local_dir = config['configurations']['storm-site']['storm.local.dir']
user_group = config['configurations']['global']['user_group']
java64_home = config['hostLevelParams']['java_home']
nimbus_host = config['configurations']['storm-site']['nimbus.host']
nimbus_port = config['configurations']['storm-site']['nimbus.thrift.port']
rest_api_conf_file = format("{conf_dir}/config.yaml")
rest_lib_dir = format("{app_root}/external/storm-rest")
storm_bin = format("{app_root}/bin/storm.py")
storm_env_sh_template = config['configurations']['storm-env']['content']

metric_collector_host = default('/configurations/global/metric_collector_host', '')
metric_collector_port = default('/configurations/global/metric_collector_port', '')
metric_collector_lib = default('/configurations/global/metric_collector_lib', '')
metric_collector_app_id = format("{app_name}")

has_metric_collector = 1
if not metric_collector_lib:
  has_metric_collector = 0

security_enabled = config['configurations']['global']['security_enabled']

if security_enabled:
  _hostname_lowercase = config['hostname'].lower()
  _kerberos_domain = config['configurations']['storm-env']['kerberos_domain']
  _storm_client_principal_name = config['configurations']['storm-env']['storm_client_principal_name']
  _storm_server_principal_name = config['configurations']['storm-env']['storm_server_principal_name']

  storm_jaas_client_principal = _storm_client_principal_name.replace('_HOST', _hostname_lowercase)
  storm_client_keytab_path = config['configurations']['storm-env']['storm_client_keytab']
  storm_jaas_server_principal = _storm_server_principal_name.replace('_HOST',nimbus_host.lower())
  storm_jaas_stormclient_servicename = storm_jaas_server_principal.split("/")[0]
  storm_server_keytab_path = config['configurations']['storm-env']['storm_server_keytab']
  kinit_path_local = functions.get_kinit_path(["/usr/bin", "/usr/kerberos/bin", "/usr/sbin"])

metric_collector_sink_jar = "/usr/lib/storm/lib/ambari-metrics-storm-sink*.jar"
storm_lib_dir = format("{app_root}/lib")
