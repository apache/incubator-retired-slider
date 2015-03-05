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

class AccumuloClient(Script):
  def install(self, env):
    import client_params
    env.set_params(client_params)
    self.install_packages(env)
    Directory(client_params.conf_dir,
              content=format("{conf_dir}/templates"))
    jarname = "SliderAccumuloUtils.jar"
    File(format("{client_root}/lib/{jarname}"),
         mode=0644,
         content=StaticFile(jarname)
    )
    File(format("{bin_dir}/accumulo-slider"),
         content=StaticFile("accumulo-slider"),
         mode=0755
    )
    File(format("{bin_dir}/accumulo-slider.py"),
         content=StaticFile("accumulo-slider.py"),
         mode=0755
    )
    TemplateConfig(format("{conf_dir}/accumulo-slider-env.sh"),
                   mode=0755
    )
    if client_params.app_name:
      Execute( format("{bin_dir}/accumulo-slider "
                      "--appconf {client_root}/conf --app {app_name} getconf "))

  def configure(self, env):
    pass

  def start(self, env):
    pass

  def stop(self, env):
    pass

  def status(self, env):
    pass


if __name__ == "__main__":
  AccumuloClient().execute()
