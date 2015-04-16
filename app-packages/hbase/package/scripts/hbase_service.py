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

def hbase_service(
  name,
  action = 'start'): # 'start' or 'stop' or 'status'
    
    import params
  
    role = name
    cmd = format("{daemon_script} --config {conf_dir}")
    pid_file = format("{pid_dir}/hbase-{hbase_user}-{role}.pid")
    
    daemon_cmd = None
    no_op_test = None
    
    if action == 'start':
      daemon_cmd = format("env HBASE_IDENT_STRING={hbase_user} {cmd} start {role}")
      if name == 'rest':
        infoport = ""
        if not params.rest_infoport == "":
          infoport = " --infoport {params.rest_infoport}"
        readonly = ""
        if not params.rest_readonly == "":
          readonly = " --readonly"
        daemon_cmd = format("{daemon_cmd} -p {rest_port}" + infoport + readonly)
      elif name == 'thrift':
        queue = ""
        if not params.thrift_queue == "":
          queue = " -q {params.thrift_queue}"
        workers = ""
        if not params.thrift_workers == "":
          workers = " -w {params.thrift_workers}"
        compact = ""
        if not params.thrift_compact == "":
          compact = " -c"
        framed = ""
        if not params.thrift_framed == "":
          framed = " -f"
        infoport = ""
        if not params.thrift_infoport == "":
          infoport = " --infoport {params.thrift_infoport}"
        keepalive_sec = ""
        if not params.thrift_keepalive_sec == "":
          keepalive_sec = " --keepAliveSec {params.thrift_keepalive_sec}"
        minWorkers = ""
        if not params.thrift_minWorkers == "":
          minWorkers = " --minWorkers {params.thrift_minWorkers}"
        nonblocking = ""
        if not params.thrift_nonblocking == "":
          nonblocking = " -nonblocking"
        daemon_cmd = format("{daemon_cmd} -p {thrift_port}" + queue + workers + compact + framed + infoport + keepalive_sec + minWorkers + nonblocking)
      elif name == 'thrift2':
        compact = ""
        if not params.thrift2_compact == "":
          compact = " -c"
        framed = ""
        if not params.thrift2_framed == "":
          framed = " -f"
        infoport = ""
        if not params.thrift2_infoport == "":
          infoport = " --infoport {params.thrift2_infoport}"
        nonblocking = ""
        if not params.thrift2_nonblocking == "":
          nonblocking = " -nonblocking"
        daemon_cmd = format("{daemon_cmd} -p {thrift2_port}" + compact + framed + infoport + nonblocking)
      no_op_test = format("ls {pid_file} >/dev/null 2>&1 && ps `cat {pid_file}` >/dev/null 2>&1")
    elif action == 'stop':
      print format("Stop called for {role}")
      daemon_cmd = format("env HBASE_IDENT_STRING={hbase_user} {cmd} stop {role} && rm -f {pid_file}")
    elif action == 'pre_upgrade':
      print format("Pre upgrade {role} - do something short and useful here")
    elif action == 'post_upgrade':
      print format("Post upgrade {role} - currently not plugged in")

    if daemon_cmd is not None:
      Execute ( daemon_cmd,
        not_if = no_op_test
      )
