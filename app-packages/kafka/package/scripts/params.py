# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from resource_management import *

config = Script.get_config()


app_root = config['configurations']['global']['app_root']
java64_home = config['hostLevelParams']['java_home']
app_user = config['configurations']['global']['app_user']
pid_file = config['configurations']['global']['pid_file']
app_log_dir = config['configurations']['global']['app_log_dir']
kafka_version = config['configurations']['global']['kafka_version']
xmx = config['configurations']['broker']['xmx_val']
xms = config['configurations']['broker']['xms_val']

conf_dir = format("{app_root}/config")

broker_config=dict(line.strip().split('=') for line in open(format("{conf_dir}/server.properties")) if not (line.startswith('#') or re.match(r'^\s*$', line)))
broker_config.update(config['configurations']['server'])
