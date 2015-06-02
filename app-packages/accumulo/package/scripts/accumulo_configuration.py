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

def setup_conf_dir(name=None): # 'master' or 'tserver' or 'monitor' or 'gc' or 'tracer' or 'client'
  import params

  # create the conf directory
  Directory( params.conf_dir,
      owner = params.accumulo_user,
      group = params.user_group,
      recursive = True
  )

  jarname = "SliderAccumuloUtils.jar"
  File(format("{params.accumulo_root}/lib/{jarname}"),
       mode=0644,
       group=params.user_group,
       owner=params.accumulo_user,
       content=StaticFile(jarname)
  )

  # create pid dir
  Directory( params.pid_dir,
    owner = params.accumulo_user,
    group = params.user_group,
    recursive = True
  )

  # create log dir
  Directory (params.log_dir,
    owner = params.accumulo_user,
    group = params.user_group,
    recursive = True
  )

  # create a site file for server processes
  XmlConfig( "accumulo-site.xml",
          conf_dir = params.conf_dir,
          configurations = params.config['configurations']['accumulo-site'],
          owner = params.accumulo_user,
          group = params.user_group,
          mode=0600
  )

  # create env file
  File(format("{params.conf_dir}/accumulo-env.sh"),
       mode=0644,
       group=params.user_group,
       owner=params.accumulo_user,
       content=InlineTemplate(params.env_sh_template)
  )

  # create metrics2 properties file
  accumulo_TemplateConfig('hadoop-metrics2-accumulo.properties')

  if name == "proxy":
    # create proxy.properties file
    PropertiesFile(params.proxy_conf,
                   properties = params.config['configurations']['proxy'],
                   owner = params.accumulo_user,
                   group = params.user_group
    )
    # create client.conf file
    configs = {}
    configs.update(params.config['configurations']['client'])
    update_site_config(configs, 'general.security.credential.provider.paths')
    update_site_config(configs, 'rpc.javax.net.ssl.trustStore')
    update_site_config(configs, 'rpc.javax.net.ssl.trustStoreType')
    update_site_config(configs, 'rpc.javax.net.ssl.keyStore')
    update_site_config(configs, 'rpc.javax.net.ssl.keyStoreType')
    for key,value in params.config['configurations']['accumulo-site'].iteritems():
      if key.startswith("trace.span.receiver."):
        configs[key] = value
    PropertiesFile(format("{params.conf_dir}/client.conf"),
                   properties = configs,
                   owner = params.accumulo_user,
                   group = params.user_group
                   )

  # create host files
  accumulo_StaticFile( 'masters')
  accumulo_StaticFile( 'slaves')
  accumulo_StaticFile( 'monitor')
  accumulo_StaticFile( 'gc')
  accumulo_StaticFile( 'tracers')

  # create log4j.properties files
  if (params.log4j_props != None):
    File(format("{params.conf_dir}/log4j.properties"),
         mode=0644,
         group=params.user_group,
         owner=params.accumulo_user,
         content=params.log4j_props
    )
  else:
    accumulo_StaticFile("log4j.properties")

  # create other logging configuration files
  accumulo_StaticFile("auditLog.xml")
  accumulo_StaticFile("generic_logger.xml")
  accumulo_StaticFile("monitor_logger.xml")

  # create the policy file
  if 'accumulo-policy' in params.config['configurations']:
    XmlConfig( "accumulo-policy.xml",
      configurations = params.config['configurations']['accumulo-policy'],
      owner = params.accumulo_user,
      group = params.user_group
    )

# update configs
def update_site_config(configs, name):
  import params
  if name in params.config['configurations']['accumulo-site']:
    configs[name] = params.config['configurations']['accumulo-site'][name]

# create file 'name' from template
def accumulo_TemplateConfig(name,
                         tag=None
                         ):
  import params

  TemplateConfig( format("{params.conf_dir}/{name}"),
      owner = params.accumulo_user,
      group = params.user_group,
      template_tag = tag
  )

# create static file 'name'
def accumulo_StaticFile(name):
  import params

  File(format("{params.conf_dir}/{name}"),
    mode=0644,
    group=params.user_group,
    owner=params.accumulo_user,
    content=StaticFile(name)
  )
