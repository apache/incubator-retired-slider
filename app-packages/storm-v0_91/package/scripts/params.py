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
conf_dir = format("{app_root}/conf")
storm_user = config['configurations']['global']['app_user']
log_dir = config['configurations']['global']['app_log_dir']
pid_dir = status_params.pid_dir
local_dir = config['configurations']['storm-site']['storm.local.dir']
user_group = config['configurations']['global']['user_group']
java64_home = config['hostLevelParams']['java_home']
nimbus_host = config['configurations']['storm-site']['nimbus.host']
nimbus_port = config['configurations']['storm-site']['nimbus.thrift.port']
nimbus_host = config['configurations']['storm-site']['nimbus.host']
rest_api_port = config['configurations']['global']['rest_api_port']
rest_api_admin_port = config['configurations']['global']['rest_api_admin_port']
rest_api_conf_file = format("{conf_dir}/config.yaml")
rest_lib_dir = format("{app_root}/contrib/storm-rest")
storm_bin = format("{app_root}/bin/storm")

ganglia_installed = config['configurations']['global']['ganglia_enabled']
if ganglia_installed:
  ganglia_report_interval = 60
  ganglia_server = config['configurations']['global']['ganglia_server_host']
  ganglia_port = config['configurations']['global']['ganglia_server_port']

_authentication = config['configurations']['core-site']['hadoop.security.authentication']
security_enabled = ( not is_empty(_authentication) and _authentication == 'kerberos')

if security_enabled:
  _hostname_lowercase = config['hostname'].lower()
  _kerberos_domain = config['configurations']['global']['kerberos_domain']
  _storm_principal_name = config['configurations']['global']['storm_principal_name']
  storm_jaas_principal = _storm_principal_name.replace('_HOST', _hostname_lowercase)
  storm_keytab_path = config['configurations']['global']['storm_keytab']
