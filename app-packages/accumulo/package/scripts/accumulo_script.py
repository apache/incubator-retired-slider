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
from resource_management.core.environment import Environment

from accumulo_configuration import setup_conf_dir
from accumulo_configuration import accumulo_StaticFile
from accumulo_service import accumulo_service


class AccumuloScript(Script):
  def __init__(self, component):
    self.component = component

  def install(self, env):
    self.install_packages(env)

  def configure(self, env):
    import params
    env.set_params(params)

    if params.monitor_security_enabled and self.component == 'monitor':
      import os
      import random
      import string

      basedir = Environment.get_instance().config.basedir
      keystore_file = os.path.join(basedir, "files", "keystore.jks")
      truststore_file = os.path.join(basedir, "files", "cacerts.jks")
      cert_file = os.path.join(basedir, "files", "server.cer")

      if os.path.exists(keystore_file) or os.path.exists(truststore_file) or os.path.exists(cert_file):
        self.fail_with_error("trying to create monitor certs but they already existed")

      goodchars = string.lowercase + string.uppercase + string.digits + '#%+,-./:=?@^_'
      keypass = ''.join(random.choice(goodchars) for x in range(20))
      storepass = ''.join(random.choice(goodchars) for x in range(20))

      https_params = {}
      https_params[params.keystore_property] = params.keystore_path
      https_params[params.truststore_property] = params.truststore_path
      https_params[params.keystore_password_property] = keypass
      https_params[params.truststore_password_property] = storepass

      setup_conf_dir(name=self.component, extra_params=https_params)

      Execute( format("{java64_home}/bin/keytool -genkey -alias \"default\" -keyalg RSA -keypass {keypass} -storepass {storepass} -keystore {keystore_file} -dname \"CN=Unknown, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown\""),
               user=params.accumulo_user)
      Execute( format("{java64_home}/bin/keytool -export -alias \"default\" -storepass {storepass} -file {cert_file} -keystore {keystore_file}"),
               user=params.accumulo_user)
      Execute( format("echo \"yes\" | {java64_home}/bin/keytool -import -v -trustcacerts -alias \"default\" -file {cert_file} -keystore {truststore_file} -keypass {keypass} -storepass {storepass}"),
               user=params.accumulo_user)

      accumulo_StaticFile("keystore.jks")
      accumulo_StaticFile("cacerts.jks")

    else:
      setup_conf_dir(name=self.component)


  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env) # for security

    if self.component == 'master':
      try:
        Execute( format("{daemon_script} init --instance-name {accumulo_instance_name} --password {accumulo_root_password} --clear-instance-name >{log_dir}/accumulo-{accumulo_user}-init.out 2>{log_dir}/accumulo-{accumulo_user}-init.err"),
               not_if=format("hadoop fs -stat {accumulo_hdfs_root_dir}"),
               user=params.accumulo_user)
      except Exception, e:
        try:
          Execute( format("hadoop fs -rm -R {accumulo_hdfs_root_dir}"),
               user=params.accumulo_user)
        except:
          pass
        raise e

    accumulo_service( self.component,
      action = 'start'
    )

  def stop(self, env):
    import params
    env.set_params(params)

    accumulo_service( self.component,
      action = 'stop'
    )

  def status(self, env):
    import status_params
    env.set_params(status_params)
    component = self.component
    pid_file = format("{pid_dir}/accumulo-{accumulo_user}-{component}.pid")
    check_process_status(pid_file)


if __name__ == "__main__":
  self.fail_with_error('component unspecified')
