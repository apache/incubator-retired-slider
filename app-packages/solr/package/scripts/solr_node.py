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

class Solr_Component(Script):
  def install(self, env):
    self.install_packages(env)

  def configure(self, env):
    import params
    env.set_params(params)

  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env)
    start_solr_cmd = """{java64_home}/bin/java
-server
-Xss256k
-Xmx{xmx_val}
-Xms{xms_val}
{gc_tune}
{solr_opts}
-DzkClientTimeout={zk_timeout}
-DzkHost={zk_host}
-Dhost={solr_host}
-Djetty.port={port}
-DSTOP.PORT={stop_port}
-DSTOP.KEY={stop_key}
-Duser.timezone=UTC
-Dsolr.solr.home=\"{app_root}/server/solr\"
-Dsolr.install.dir=\"{app_root}\"
-Djetty.home=\"{app_root}/server\"
-Xloggc:\"{app_root}/server/logs/solr_gc.log\"
-jar start.jar {server_module}"""

    process_cmd = format(start_solr_cmd.replace("\n", " "))
    print("Starting Solr using command: "+process_cmd)
    Execute(process_cmd,
        logoutput=True,
        wait_for_finish=False,
        pid_file=params.pid_file,
        poll_after = 10,
        cwd=format("{app_root}/server")
    )


  def stop(self, env):
    import params
    env.set_params(params)
    stop_cmd = format("bin/solr stop -p {port} -k {stop_key}")
    Execute(stop_cmd,
            logoutput=True,
            wait_for_finish=True,
            cwd=format("{app_root}")
    )

  def status(self, env):
    import params
    env.set_params(params)
    status_cmd = "bin/solr status"
    Execute(status_cmd,
        logoutput=True,
        wait_for_finish=True,
        cwd=format("{app_root}") 
    )

if __name__ == "__main__":
  Solr_Component().execute()
