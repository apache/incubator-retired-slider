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

class Tomcat(Script):
  def install(self, env):
    self.install_packages(env)
    pass

  def configure(self, env):
    import params
    env.set_params(params)
    File(format("{conf_dir}/server.xml"), content=Template("server.xml.j2"))

  def start(self, env):
    import params
    self.configure(env)
    tomcat_pid = format('{app_root}/catalina.pid')
    process_cmd = format('env JAVA_HOME={java64_home} CATALINA_PID=' + tomcat_pid + ' {app_root}/apache-tomcat-*/bin/catalina.sh start')

    Execute(process_cmd,
        logoutput=False,
        wait_for_finish=True,
        pid_file=tomcat_pid,
        poll_after = 15
    )

  def stop(self, env):
    import params
    self.configure(env)
    tomcat_pid = format('{app_root}/catalina.pid')
    process_cmd = format('env JAVA_HOME={java64_home} CATALINA_PID=' + tomcat_pid + ' {app_root}/apache-tomcat-*/bin/catalina.sh stop')

    Execute(process_cmd,
        logoutput=True,
        wait_for_finish=True,
        pid_file=tomcat_pid,
        poll_after = 15
    )

  def status(self, env):
    import params
    self.configure(env)
    tomcat_pid = format('{app_root}/catalina.pid')
    check_process_status(tomcat_pid)

if __name__ == "__main__":
  Tomcat().execute()
