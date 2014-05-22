<!---
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

#App Instance Configuration

App Instance Configuration is the configuration override provided by the application owner when creating an application instance using Slider. This configuration values override the default configuration available in the App Package.

Instance configuration is a JSON formatted doc in the following form:


    {
      "schema": "http://example.org/specification/v2.0.0",
      "metadata": {
      },
      "global": {            
      },
    }

An appConfig.json contains the application configuration. The sample below shows configuration for HBase.


    {
      "schema" : "http://example.org/specification/v2.0.0",
      "metadata" : {
      },
      "global" : {
          "config_types": "core-site,hdfs-site,hbase-site",
          
          "java_home": "/usr/jdk64/jdk1.7.0_45",
          "package_list": "files/hbase-0.96.1-hadoop2-bin.tar",
          
          "site.global.app_user": "yarn",
          "site.global.app_log_dir": "${AGENT_LOG_ROOT}/app/log",
          "site.global.app_pid_dir": "${AGENT_WORK_ROOT}/app/run",
          "site.global.security_enabled": "false",
  
          "site.hbase-site.hbase.hstore.flush.retries.number": "120",
          "site.hbase-site.hbase.client.keyvalue.maxsize": "10485760",
          "site.hbase-site.hbase.hstore.compactionThreshold": "3",
          "site.hbase-site.hbase.rootdir": "${NN_URI}/apps/hbase/data",
          "site.hbase-site.hbase.tmp.dir": "${AGENT_WORK_ROOT}/work/app/tmp",
          "site.hbase-site.hbase.master.info.port": "${HBASE_MASTER.ALLOCATED_PORT}",
          "site.hbase-site.hbase.regionserver.port": "0",
  
          "site.core-site.fs.defaultFS": "${NN_URI}",
          "site.hdfs-site.dfs.namenode.https-address": "${NN_HOST}:50470",
          "site.hdfs-site.dfs.namenode.http-address": "${NN_HOST}:50070"
      }
    }

appConf.json allows you to pass in arbitrary set of configuration that Slider will forward to the application component instances.

**Variable naming convention**
In order to understand how the naming convention work, lets look at how the config is passed on to component commands. Slider agent recevies a structured bag of commands as input for all commands, INSTALL, CONFIGURE, START, etc. The command includes a section "configuration" which has config properties arranged into named property bags.

* Variables of the form `site.xx.yy` translates to variables by the name `yy` within the group `xx` and are typically converted to site config files by the name `xx` containing variable `yy`. For example, `"site.hbase-site.hbase.regionserver.port":""` will be sent to the Slider-Agent as `"hbase-site" : { "hbase.regionserver.port": ""}` and app definition scripts can access all variables under `hbase-site` as a single property bag.
* Similarly, `site.core-site.fs.defaultFS` allows you to pass in the default fs. *This specific variable is automatically made available by Slider but its shown here as an example.*
* Variables of the form `site.global.zz` are sent in the same manner as other site variables except these variables are not expected to get translated to a site xml file. Usually, variables needed for template or other filter conditions (such as security_enabled = true/false) can be sent in as "global variable". 

**slider variables**

* Any config not of the form `site.xx.yy` are consumed by Slider itself. Some of the manadatory configuration are:
  * `agent.conf`: location of the agent config file (typically, "/slider/agent/conf/agent.ini")
  * `application.def`: location of the application definition package (typically, "/slider/hbase_v096.zip")
  * `config_types`: list of config types sent to the containers (e.g. "core-site,hdfs-site,hbase-site")
  * `java_home`: java home path (e.g. "/usr/jdk64/jdk1.7.0_45")
  * `package_list`: location of the package relative to the root where AppPackage is installed (e.g. "files/hbase-0.96.1-hadoop2-bin.tar.gz"

**dynamically allocated ports**

Apps can ask port to be dynamically assigned by Slider or they can leave it as "0". If there is a need for advertising any listening endpoint then the ports can be marked such.

For example, HBase master info port needs to be advertised so that jmx endpoint can be accessed. This is indicated by using a special value, of the form, ${COMPONENT_NAME.ALLOCATED_PORT}. E.g. "site.hbase-site.hbase.master.info.port": "${HBASE_MASTER.ALLOCATED_PORT}"

[Application Definition](application_definition.md) describes how to advertise arbitrary set of properties that are dynamically finalized when application is activated.

**configuraing an app for ganglia metrics**

There is no set guideline for doing so. How an application emits metrics and how the metrics are emitted to the right place is completely defined by the application. In the following example, we hso how HBase app is configured to emit metrics to a ganglia server.

Ganglia server lifecycle is not controlled by the app instance. So the app instance only needs to know where to emit the metrics. This is achieved by three global variables

* "site.global.ganglia_server_host": "gangliaserver.my.org"
* "site.global.ganglia_server_port": "8663"
* "site.global.ganglia_server_id": "HBaseApplicationCluster3"

All three variable values are user provided. It is also expected that a gmond server is available on host gangliaserver.my.org listening for metrics at port 8663 and is named "HBaseApplicationCluster3". Its the reponsibility of the ganglia server admin to ensure that the server is unique and is only receving metrics from the application instance.



