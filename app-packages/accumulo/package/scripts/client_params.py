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
import os

config = Script.get_config()

app_install_dir = config['configurations']['global']['app_install_dir']
client_root = config['configurations']['global']['client_root']
bin_dir = os.path.join(client_root, 'bin')
conf_dir = os.path.join(client_root, 'conf')

app_name = None
if 'app_name' in config['configurations']['global']:
  app_name = config['configurations']['global']['app_name']

slider_home_dir = config['configurations']['global']['slider_home_dir']
if os.environ.has_key('SLIDER_CONF_DIR'):
  slider_conf_dir = os.environ.get('SLIDER_CONF_DIR')
else:
  slider_conf_dir = os.path.join(slider_home_dir, 'conf')
