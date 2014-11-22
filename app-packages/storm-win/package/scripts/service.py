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
import sys
import xml.etree.ElementTree as et

"""
Slider package uses jps as pgrep does not list the whole process start command
"""
def service(
    name,
    action='start'):
  import params
  import status_params

  pid_file = status_params.pid_files[name]
  backtype = format("backtype.storm.daemon.{name}")

  if action == "start":
    os.environ['STORM_LOG_DIR'] = params.log_dir
    os.environ['STORM_HOME'] = params.app_root
    os.environ['STORM_CONF_DIR'] = params.conf_dir

    generate_xml = format("{storm_bin} --service {name} > {log_dir}/{name}.cmd")

    Execute(generate_xml,
            logoutput=True,
            wait_for_finish=True
    )

    tree = et.parse(format("{log_dir}/{name}.cmd"))
    root = tree.getroot()
    cmd_part = None
    for child in root:
      if child.tag == "arguments":
        cmd_part = child.text

    if cmd_part:
      cmd = format("{java64_home}\\bin\\java {cmd_part}")

      Execute(cmd,
              logoutput=False,
              wait_for_finish=False,
              pid_file=pid_file
      )
    else:
      Logger.warn("Valid command file did not get generated at " + format("{log_dir}/{name}.cmd"))

  elif action == "stop":
    pid = format("`cat {pid_file}` >/dev/null 2>&1")
    Execute(format("kill {pid}")
    )
    Execute(format("kill -9 {pid}"),
            ignore_failures=True
    )
    Execute(format("rm -f {pid_file}"))
