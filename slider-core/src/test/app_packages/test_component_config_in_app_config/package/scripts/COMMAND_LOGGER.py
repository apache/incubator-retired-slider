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

import sys
import subprocess
from resource_management import *
import tempfile
         
class COMMAND_LOGGER(Script):
  def install(self, env):
    config = Script.get_config()
    filename = config['componentConfig']['COMMAND_LOGGER']['file_name']
    self.install_packages(env)
    f = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
    TMP_LOCAL_FILE = f.name
    try:
      f.write("testing...")
      print TMP_LOCAL_FILE
    finally:
      f.close()
    cat = subprocess.Popen("hdfs dfs -copyFromLocal " + TMP_LOCAL_FILE + " /tmp/" + filename, shell=True)
    cat.communicate()
    print "running install for all components in add on pkg"
    
  def configure(self, env):
    import params
    env.set_params(params)
    
  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env) # for security

    
  def stop(self, env):
    import params
    env.set_params(params)


  def status(self, env):
    pass

if __name__ == "__main__":
  COMMAND_LOGGER().execute()
  pass
