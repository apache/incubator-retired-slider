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

# server configurations
config = Script.get_config()

app_root = config['configurations']['global']['app_root']
app_version = config['configurations']['global']['app_version']
java64_home = config['hostLevelParams']['java_home']
pid_file = config['configurations']['global']['pid_file']

additional_cp = config['configurations']['global']['additional_cp']
xmx_val = config['configurations']['global']['xmx_val']
xms_val = config['configurations']['global']['xms_val']
memory_val = config['configurations']['global']['memory_val']
port = config['configurations']['global']['listen_port']
