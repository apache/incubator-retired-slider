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

  ssl_params = False
  if params.ssl_enabled or (params.monitor_security_enabled and
                                name == 'monitor'):
    import os

    ssl_params = True
    if os.path.exists(params.keystore_path) or os.path.exists(params.truststore_path):
      if os.path.exists(params.keystore_path) and os.path.exists(params.truststore_path):
        # assume keystores were already set up properly
        pass
      else:
        self.fail_with_error("something went wrong when certs were created")

    Directory( format("{params.conf_dir}/ssl"),
               owner = params.accumulo_user,
               group = params.user_group,
               recursive = True
    )
    if not os.path.exists(params.truststore_path):
      Execute( format("{hadoop_prefix}/bin/hadoop fs -get {params.ssl_cert_dir}/truststore.jks "
                      "{params.truststore_path}"),
               user=params.accumulo_user)
      File( params.truststore_path,
            mode=0600,
            group=params.user_group,
            owner=params.accumulo_user,
            replace=False)
    if not os.path.exists(params.keystore_path):
      Execute( format("{hadoop_prefix}/bin/hadoop fs -get {params.ssl_cert_dir}/{params.hostname}.jks "
                      "{params.keystore_path}"),
               user=params.accumulo_user)
      File( params.keystore_path,
            mode=0600,
            group=params.user_group,
            owner=params.accumulo_user,
            replace=False)

  jarname = "SliderAccumuloUtils.jar"
  File(format("{params.accumulo_root}/lib/{jarname}"),
       mode=0644,
       group=params.user_group,
       owner=params.accumulo_user,
       content=StaticFile(jarname)
  )

  if name != "client":
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

    configs = {}
    if ssl_params:
      configs.update(params.config['configurations']['accumulo-site'])
      if (params.monitor_security_enabled and name == 'monitor'):
        configs[params.monitor_keystore_property] = params.keystore_path
        configs[params.monitor_truststore_property] = params.truststore_path
      if params.ssl_enabled:
        configs[params.ssl_keystore_file_property] = params.keystore_path
        configs[params.ssl_truststore_file_property] = params.truststore_path
    else:
      configs = params.config['configurations']['accumulo-site']

    # create a site file for server processes
    XmlConfig( "accumulo-site.xml",
            conf_dir = params.conf_dir,
            configurations = configs,
            owner = params.accumulo_user,
            group = params.user_group,
            mode=0600
    )
  else:
    # create a minimal site file for client processes
    client_configurations = {}
    client_configurations['instance.zookeeper.host'] = params.config['configurations']['accumulo-site']['instance.zookeeper.host']
    client_configurations['instance.volumes'] = params.config['configurations']['accumulo-site']['instance.volumes']
    client_configurations['general.classpaths'] = params.config['configurations']['accumulo-site']['general.classpaths']
    XmlConfig( "accumulo-site.xml",
            conf_dir = params.conf_dir,
            configurations = client_configurations,
            owner = params.accumulo_user,
            group = params.user_group
    )

  # create env file
  accumulo_TemplateConfig( 'accumulo-env.sh')

  # create client.conf file
  PropertiesFile(format("{params.conf_dir}/client.conf"),
       properties = params.config['configurations']['client'],
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
  accumulo_StaticFile("accumulo-metrics.xml")

  # create the policy file
  if 'accumulo-policy' in params.config['configurations']:
    XmlConfig( "accumulo-policy.xml",
      configurations = params.config['configurations']['accumulo-policy'],
      owner = params.accumulo_user,
      group = params.user_group
    )

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
