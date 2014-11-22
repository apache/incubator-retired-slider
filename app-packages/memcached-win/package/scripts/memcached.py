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
from resource_management import *

class Memcached(Script):
  def install(self, env):
    self.install_packages(env)
    pass

  def configure(self, env):
    import params
    env.set_params(params)

  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env)
    process_cmd = format("{java64_home}\\bin\\java -Xmx{xmx_val} -Xms{xms_val} -classpath {app_root}\\*;{additional_cp} com.thimbleware.jmemcached.Main --memory={memory_val} --port={port}")

    Execute(process_cmd,
        logoutput=False,
        wait_for_finish=False,
        pid_file=params.pid_file,
        poll_after = 5
    )

  def stop(self, env):
    import params
    env.set_params(params)

  def status(self, env):
    import params
    env.set_params(params)
    check_process_status(params.pid_file)

if __name__ == "__main__":
  Memcached().execute()
