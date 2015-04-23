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
import json

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
    TemplateConfig(format("{client_conf}/accumulo-slider-env.sh"),
                   mode=0755
    )
    if client_params.app_name:
      Logger.info("Creating configs for app %s" % client_params.app_name)
      Directory(client_params.conf_download_dir)
      Execute( format("SLIDER_CONF_DIR={slider_conf_dir} "
                      "{slider_home_dir}/bin/slider registry --getconf "
                      "accumulo-env --format env "
                      "--dest {conf_download_dir}/accumulo-env.sh "
                      "--name {app_name}"))
      Execute( format("SLIDER_CONF_DIR={slider_conf_dir} "
                      "{slider_home_dir}/bin/slider registry --getconf "
                      "accumulo-site --format json "
                      "--dest {conf_download_dir}/accumulo-site.xml "
                      "--name {app_name}"))
      with open(format("{conf_download_dir}/accumulo-site.xml"),"r") as fp:
        site = json.load(fp)
        sensitive_props = ["instance.secret",
                           "general.security.credential.provider.paths",
                           "rpc.javax.net.ssl.keyStorePassword",
                           "rpc.javax.net.ssl.trustStorePassword",
                           "monitor.ssl.keyStorePassword",
                           "monitor.ssl.trustStorePassword",
                           "trace.password",
                           "trace.token.property.password"]
        for prop in sensitive_props:
          site.pop(prop, None)
      XmlConfig( "accumulo-site.xml",
                 conf_dir = client_params.conf_download_dir,
                 configurations=site,
                 mode=0644)
      Execute( format("SLIDER_CONF_DIR={slider_conf_dir} "
                      "{slider_home_dir}/bin/slider registry --getconf "
                      "client --format properties "
                      "--dest {conf_download_dir}/client.conf "
                      "--name {app_name}"))
      configs = {}
      with open(format("{conf_download_dir}/client.conf"),"r") as fp:
        content = fp.readlines()
        for line in content:
          index = line.find("=")
          if index > 0:
            configs[line[0:index]] = line[index+1:].rstrip()
      for key, value in site.iteritems():
        if key.startswith("trace.span.receiver."):
          configs[key] = value
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
      PropertiesFile(format("{conf_download_dir}/client.conf"),
                     properties = configs)
      Directory(client_params.client_conf,
                content=format("{client_conf}/templates"))
      Directory(client_params.client_conf,
                content=client_params.conf_download_dir)
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
