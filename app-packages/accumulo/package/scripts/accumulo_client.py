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
  def check_provider_contains(self, provider, alias):
    try:
      Execute( format("hadoop credential list -provider {provider} | "
                      "grep -i {alias}"))
    except:
      raise Fail(format("{provider} did not contain {alias}, try running "
                        "'hadoop credential create {alias} -provider "
                        "{provider}' or configure SSL certs manually"))

  def install(self, env):
    import client_params
    env.set_params(client_params)
    self.install_packages(env)
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
      Logger.info("Creating configs for app %s" % client_params.app_name)
      Directory(client_params.conf_dir,
                content=format("{conf_dir}/templates"))
      Execute( format("{bin_dir}/accumulo-slider "
                      "--appconf {client_root}/conf --app {app_name} getconf "))
      configs = {}
      with open(format("{client_root}/conf/client.conf"),"r") as fp:
        content = fp.readlines()
        for line in content:
          index = line.find("=")
          if index > 0:
            configs[line[0:index]] = line[index+1:].rstrip()
      if 'instance.rpc.ssl.enabled' in configs and configs['instance.rpc.ssl.enabled']=='true':
        Logger.info("Configuring client SSL")
        self.check_provider_contains(client_params.credential_provider,
                                     client_params.keystore_alias)
        self.check_provider_contains(client_params.credential_provider,
                                     client_params.truststore_alias)
        configs['general.security.credential.provider.paths'] = client_params.credential_provider
        configs['rpc.javax.net.ssl.keyStore'] = client_params.keystore_path
        configs['rpc.javax.net.ssl.keyStoreType'] = client_params.store_type
        configs['rpc.javax.net.ssl.trustStore'] = client_params.truststore_path
        configs['rpc.javax.net.ssl.trustStoreType'] = client_params.store_type
        PropertiesFile(format("{client_root}/conf/client.conf"),
                       properties = configs
        )
        Execute( format("SLIDER_CONF_DIR={slider_conf_dir} "
                        "{slider_home_dir}/bin/slider client --getcertstore "
                        "--keystore {keystore_path} "
                        "--name {app_name} --alias {keystore_alias} "
                        "--provider {credential_provider}"))
        Execute( format("SLIDER_CONF_DIR={slider_conf_dir} "
                        "{slider_home_dir}/bin/slider client --getcertstore "
                        "--truststore {truststore_path} "
                        "--name {app_name} --alias {truststore_alias} "
                        "--provider {credential_provider}"))
    else:
      Logger.info("No app name provided, leaving client install unconfigured")


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
