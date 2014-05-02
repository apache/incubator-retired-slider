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

# Define and use Slider AppPackage

Slider AppPackages are a declarative definition of an application for application management. AppPackage is not a packaging scheme for application binaries and artifacts. Tarball, zip files, rpms etc. are available for that purpose. Instead AppPackage includes the application binaries along with other artifacts necessary for application management.

An application instance consists of several active component such as one or more masters and several slaves. There may be a number of accompanying processes in addition to the basic master/slave processes - lets refer to all processes as app component instances. When run in the context of Yarn, the application specific processes are activated within individual Yarn Container. If you pry into an Yarn container (created through Slider) it will be apparent as to what is the role of Slider-Agent and the actual application components. The following image provides an high-level view. Within a container there are at least two processes - and instance of a slider agent and an instance of an application component. The application can itself spawn more procsses if needed.

![Image](../images/slider-container.png?raw=true)

Figure 1 - High-level view of a container

For example:
	
* yarn      8849  -- python ./infra/agent/slider-agent/agent/main.py --label container_1397675825552_0011_01_000003___HBASE_REGIONSERVER --host AM_HOST --port 47830
* yarn      9085  -- bash /hadoop/yarn/local/usercache/yarn/appcache/application_1397675825552_0011/ ... internal_start regionserver
* yarn      9114 -- /usr/jdk64/jdk1.7.0_45/bin/java -Dproc_regionserver -XX:OnOutOfMemoryError=...

Shows three processes, the Slider-Agent process, the bash script to start HBase Region Server and the HBase Region server itself. Three of these together constitute the container.	

## Using an AppPackage
The following command creates an HBase application using the AppPackage for HBase.

	"./slider create cl1 --zkhosts zk1,zk2 --image hdfs://NN:8020/slider/agent/slider-agent-0.21.tar --option agent.conf hdfs://NN:8020/slider/agent/conf/agent.ini  --template /work/appConf.json --resources /work/resources.json  --option application.def hdfs://NN:8020/slider/hbase_v096.tar"
	
Lets analyze various parameters from the perspective of app creation:
  
* **--image**: its the slider agent tarball
* **--option agent.conf**: the configuration file for the agent instance
* **--option app.def**: app def (AppPackage)
* **--template**: app configuration
* **--resources**: yarn resource requests
* … other parameters are described in accompanying docs. 

### AppPackage
The structure of an AppPackage is described at [AppPackage](application_package.md).

In the enlistment there are three example AppPackages

* app-packages/hbase-v0_96
* app-packages/accumulo-v1_5
* app-packages/storm-v0_91

The application tarball, containing the binaries/artifacts of the application itself is a component within the AppPackage. They are:

* For hbase-v0_96 - app-packages/hbase-v0_96/package/files/hbase-0.96.1-hadoop2-bin.tar.gz.REPLACE
* For accumulo-v1_5 - app-packages/accumulo-v1_5/package/files/accumulo-1.5.1-bin.tar.gz.REPLACE
* For storm-v0_91 - app-packages/storm-v0_91/package/files/apache-storm-0.9.1.2.1.1.0-237.tar.gz.placeholder

They are placehoder files, mostly because the files themselves are too large as well as users are free to use their own version of the package. To create a Slider AppPackage - replace the file with an actual application tarball and then ensure that the metainfo.xml has the correct file name. After that create a tarball using standard tar commands and ensure that the package has the metainfo.xml file at the root folder.

### appConf.json
An appConf.json contains the application configuration. The sample below shows configuration for HBase.

```
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
        "site.hbase-site.hbase.regionserver.port": "0",

        "site.core-site.fs.defaultFS": "${NN_URI}",
        "site.hdfs-site.dfs.namenode.https-address": "${NN_HOST}:50470",
        "site.hdfs-site.dfs.namenode.http-address": "${NN_HOST}:50070"
    }
}
```
appConf.jso allows you to pass in arbitrary set of configuration that Slider will forward to the application component instances.

* Variables of the form "site.xx.yy" translates to variables by the name "yy" within the group "xx" and are typically converted to site config files by the name "xx" containing variable "yy". For example, "site.hbase-site.hbase.regionserver.port":"" will be sent to the Slider-Agent as "hbase-site" : { "hbase.regionserver.port": ""} and app def scripts can access all variables under "hbase-site" as a single property bag.
* Similarly, "site.core-site.fs.defaultFS" allows you to pass in the default fs. *This specific variable is automatically made available by Slider but its shown here as an example.*
* Variables of the form "site.global.zz" are sent in the same manner as other site variables except these variables are not expected to get translated to a site xml file. Usually, variables needed for template or other filter conditions (such as security_enabled = true/false) can be sent in as "global variable". 

### --resources resources.json
The resources.json file encodes the Yarn resource count requirement for the application instance.

The components section lists the two application component for an HBase application.

* wait.heartbeat: a crude mechanism to control the order of component activation. A heartbeat is ~10 seconds.
* role.priority: each component must be assigned unique priority
* component.instances: number of instances for this component type
* role.script: the script path for the role *a temporary work-around as this will eventually be gleaned from metadata.xml*
            
Sample:

```
{
    "schema" : "http://example.org/specification/v2.0.0",
    "metadata" : {
    },
    "global" : {
    },
    "components" : {
        "HBASE_MASTER" : {
            "wait.heartbeat" : "5",
            "role.priority" : "1",
            "component.instances" : "1",
            "role.script" : "scripts/hbase_master.py"
        },
        "slider-appmaster" : {
            "jvm.heapsize" : "256M"
        },
        "HBASE_REGIONSERVER" : {
            "wait.heartbeat" : "3",
            "role.priority" : "2",
            "component.instances" : "1",
            "role.script" : "scripts/hbase_regionserver.py"
        }
    }
}
```

## Creating AppPackage
Refer to [App Command Scripts](writing_app_command_scripts) for details on how to write scripts for a AppPackage. These scripts are in the package/script folder within the AppPackage. *Use the checked in samples for HBase/Storm/Accumulo as reference for script development.*



