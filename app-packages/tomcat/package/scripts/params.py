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

TOMCAT_DIRECTORY_NAME = 'apache-tomcat-8.0.30'

# server configurations
config = Script.get_config()

app_root = config['configurations']['global']['app_root']
conf_dir = format("{app_root}/{TOMCAT_DIRECTORY_NAME}/conf")
java64_home = config['hostLevelParams']['java_home']
pid_file = config['configurations']['global']['pid_file']

http_port = config['configurations']['server-xml']['http.port']
connection_timeout = config['configurations']['server-xml']['connection.timeout']
ui_user = config['configurations']['tomcat-users-xml']['ui_user']
ui_password = config['configurations']['tomcat-users-xml']['ui_password']
