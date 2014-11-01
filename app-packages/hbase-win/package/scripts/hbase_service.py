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
    action='start'):  # 'start' or 'stop' or 'status'

  import params

  pid_file = format("{pid_dir}/hbase-{hbase_user}-{name}.pid")
  heap_size = params.master_heapsize
  main_class = "org.apache.hadoop.hbase.master.HMaster"
  if name == "regionserver":
    heap_size = params.regionserver_heapsize
    main_class = "org.apache.hadoop.hbase.regionserver.HRegionServer"

  role_user = format("{hbase_user}-{name}")

  rest_of_the_command = InlineTemplate(params.hbase_env_sh_template, [], heap_size=heap_size, role_user=role_user)()

  process_cmd = format("{java64_home}\\bin\\java {rest_of_the_command} {main_class} {action}")

  Execute(process_cmd,
          logoutput=False,
          wait_for_finish=False,
          pid_file=pid_file
  )