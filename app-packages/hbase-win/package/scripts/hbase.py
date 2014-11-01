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
import os

from resource_management import *
import sys
import shutil

def hbase(name=None # 'master' or 'regionserver'
              ):
  import params

  Directory( params.conf_dir,
      recursive = True,
      content = params.input_conf_files_dir
  )

  Directory (params.tmp_dir,
             recursive = True
  )

  Directory (os.path.join(params.local_dir, "jars"),
             recursive = True
  )

  XmlConfig( "hbase-site.xml",
            conf_dir = params.conf_dir,
            configurations = params.config['configurations']['hbase-site'],
            owner = params.hbase_user,
            group = params.user_group
  )

 
  if (params.log4j_props != None):
    File(format("{params.conf_dir}/log4j.properties"),
         group=params.user_group,
         owner=params.hbase_user,
         content=params.log4j_props
    )
  elif (os.path.exists(format("{conf_dir}/log4j.properties"))):
    File(format("{params.conf_dir}/log4j.properties"),
      group=params.user_group,
      owner=params.hbase_user
    )
