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
import time


def service(
    name,
    action='start'):
  import params
  import status_params

  pid_file = status_params.pid_files[name]
  no_op_test = format("ls {pid_file} >/dev/null 2>&1 && ps `cat {pid_file}` >/dev/null 2>&1")
  jps_path = format("{java64_home}/bin/jps")
  grep_and_awk = "| grep -v grep | awk '{print $1}'"

  if name == 'ui':
    #process_cmd = "^java.+backtype.storm.ui.core$"
    pid_chk_cmd = format("{jps_path} -vl | grep \"^[0-9 ]*backtype.storm.ui.core\" {grep_and_awk}  > {pid_file}")
  elif name == "rest_api":
    process_cmd = format("{java64_home}/bin/java -jar {rest_lib_dir}/`ls {rest_lib_dir} | grep -wE storm-rest-[0-9.-]+\.jar` server")
    crt_pid_cmd = format("pgrep -f \"{process_cmd}\" && pgrep -f \"{process_cmd}\" > {pid_file}")
  else:
    #process_cmd = format("^java.+backtype.storm.daemon.{name}$")
    pid_chk_cmd = format("{jps_path} -vl | grep \"^[0-9 ]*backtype.storm.daemon.{name}\" {grep_and_awk}  > {pid_file}")

  if action == "start":
    if name == "rest_api":
      cmd = format("{process_cmd} {rest_api_conf_file} > {log_dir}/restapi.log")
    else:
      cmd = format("env JAVA_HOME={java64_home} PATH=$PATH:{java64_home}/bin STORM_BASE_DIR={app_root} STORM_CONF_DIR={conf_dir} {storm_bin} {name}")

    Execute(cmd,
            not_if=no_op_test,
            user=params.storm_user,
            logoutput=False,
            wait_for_finish=False
    )

    if name == "rest_api":
      Execute(crt_pid_cmd,
              user=params.storm_user,
              logoutput=True,
              tries=6,
              try_sleep=10
      )
    else:
      content = None
      for i in xrange(12):
        Execute(pid_chk_cmd,
                user=params.storm_user,
                logoutput=True
        )
        with open(pid_file) as f:
          content = f.readline().strip()
        if content.isdigit():
          break;
        File(pid_file, action = "delete")
        time.sleep(10)
        pass

      if not content.isdigit():
        raise Fail(format("Unable to start {name}"))

  elif action == "stop":
    process_dont_exist = format("! ({no_op_test})")
    pid = format("`cat {pid_file}` >/dev/null 2>&1")
    Execute(format("kill {pid}"),
            not_if=process_dont_exist
    )
    Execute(format("kill -9 {pid}"),
            not_if=format("sleep 2; {process_dont_exist} || sleep 20; {process_dont_exist}"),
            ignore_failures=True
    )
    Execute(format("rm -f {pid_file}"))
