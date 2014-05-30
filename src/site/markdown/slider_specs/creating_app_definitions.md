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

# Apache Slider AppPackage

Slider AppPackages are a declarative definition of an application for application management. AppPackage is not a packaging scheme for application binaries and artifacts. Tarball, zip files, rpms etc. are available for that purpose. Instead AppPackage includes the application binaries along with other artifacts necessary for application management.

An application instance consists of several active component such as one or more masters and several slaves. There may be a number of accompanying processes in addition to the basic master/slave processes - lets refer to all processes as app component instances. When run in the context of Yarn, the application specific processes are activated within individual Yarn Container. If you pry into an Yarn container (created through Slider) it will be apparent as to what is the role of Slider-Agent and the actual application components. The following image provides an high-level view. Within a container there are at least two processes - and instance of a slider agent and an instance of an application component. The application can itself spawn more procsses if needed.

![Image](../../resources/images/slider-container.png?raw=true)

Figure 1 - High-level view of a container

For example:
	
    yarn      8849  -- python ./infra/agent/slider-agent/agent/main.py --label container_1397675825552_0011_01_000003___HBASE_REGIONSERVER --host AM_HOST --port 47830
    yarn      9085  -- bash /hadoop/yarn/local/usercache/yarn/appcache/application_1397675825552_0011/ ... internal_start regionserver
    yarn      9114 -- /usr/jdk64/jdk1.7.0_45/bin/java -Dproc_regionserver -XX:OnOutOfMemoryError=...

The above list shows three processes, the Slider-Agent process, the bash script to start HBase Region Server and the HBase Region server itself. *Three of these together constitute the container*.	

## Using an AppPackage
The following command creates an HBase application using the AppPackage for HBase.

	  ./slider create cl1 --image hdfs://NN:8020/slider/agent/slider-agent.tar.gz --template /work/appConf.json --resources /work/resources.json
	
Lets analyze various parameters from the perspective of app creation:
  
* `--image`: its the slider agent tarball
* `--template`: app configuration
* `--resources`: yarn resource requests
* … other parameters are described in accompanying docs. 

### AppPackage
The structure of an AppPackage is described at [AppPackage](application_package.md).

In the enlistment, there are three example AppPackages:

* `app-packages/hbase-v0_96`
* `app-packages/accumulo-v1_5`
* `app-packages/storm-v0_91`

The above folders, with minor edits, can be packaged as *zip* files to get the corresponding AppPackages. The application tarball file, containing the binaries/artifacts of the application itself is a component within the AppPackage. They are:

* For hbase - `app-packages/hbase-v0_96/package/files/hbase-0.96.1-hadoop2-bin.tar.gz.REPLACE`
* For accumulo - `app-packages/accumulo-v1_5/package/files/accumulo-1.5.1-bin.tar.gz.REPLACE`
* For storm - `app-packages/storm-v0_91/package/files/apache-storm-0.9.1.2.1.1.0-237.tar.gz.placeholder`

**They are placehoder files**, mostly because the files themselves are too large as well as users are free to use their own version of the package. To create a Slider AppPackage - replace the file with an actual application tarball and then ensure that the metainfo.xml has the correct file name. After that create a zip file using standard zip commands and ensure that the package has the metainfo.xml file at the root folder.

For example:

* cd slider/app-packages/hbase-v0_96
* zip -r hbase_v096.zip .
* Looking at the content through unzip -l "$@" hbase_v096.zip

```
Archive:  hbase_v096.zip
  Length     Date   Time    Name
 --------    ----   ----    ----
     3163  05-16-14 16:32   appConfig.json
        0  05-02-14 07:51   configuration/
     5077  05-02-14 07:51   configuration/global.xml
     5248  05-02-14 07:51   configuration/hbase-log4j.xml
     2250  05-02-14 07:51   configuration/hbase-policy.xml
    14705  05-02-14 07:51   configuration/hbase-site.xml
     3332  05-16-14 16:33   metainfo.xml
        0  05-02-14 07:51   package/
        0  05-19-14 20:36   package/files/
 83154798  05-19-14 20:36   package/files/hbase-0.96.1-hadoop2-bin.tar.gz
        0  05-02-14 07:51   package/scripts/
      787  05-02-14 07:51   package/scripts/__init__.py
     1378  05-02-14 07:51   package/scripts/functions.py
     3599  05-02-14 07:51   package/scripts/hbase.py
     1205  05-02-14 07:51   package/scripts/hbase_client.py
     1640  05-02-14 07:51   package/scripts/hbase_master.py
     1764  05-02-14 07:51   package/scripts/hbase_regionserver.py
     1482  05-02-14 07:51   package/scripts/hbase_service.py
     4924  05-02-14 07:51   package/scripts/params.py
      973  05-02-14 07:51   package/scripts/status_params.py
        0  05-02-14 07:51   package/templates/
     2723  05-02-14 07:51   package/templates/hadoop-metrics2-hbase.properties-GANGLIA-MASTER.j2
     2723  05-02-14 07:51   package/templates/hadoop-metrics2-hbase.properties-GANGLIA-RS.j2
     3878  05-02-14 07:51   package/templates/hbase-env.sh.j2
      909  05-02-14 07:51   package/templates/hbase_client_jaas.conf.j2
      989  05-02-14 07:51   package/templates/hbase_master_jaas.conf.j2
     1001  05-02-14 07:51   package/templates/hbase_regionserver_jaas.conf.j2
      837  05-02-14 07:51   package/templates/regionservers.j2
      357  05-12-14 12:04   resources.json
 --------                   -------
 83219742                   29 files
```

Sample **resources.json** and **appConfig.json** files are also included in the enlistment. These are samples and are typically tested on one node test installations.


### --template appConfig.json
An appConfig.json contains the application configuration. See [Specifications InstanceConfiguration](application_instance_configuration.md) for details on how to create a template config file. The enlistment includes sample config files for HBase, Accumulo, and Storm.


### --resources resources.json
Resource specification is an input to Slider to specify the Yarn resource needs for each component type that belong to the application. [Specification of Resources](resource_specification.html) describes how to write a resource config json file. The enlistment includes sample config files for HBase, Accumulo, and Storm.


## Scripting for AppPackage
Refer to [App Command Scripts](writing_app_command_scripts) for details on how to write scripts for a AppPackage. These scripts are in the package/script folder within the AppPackage. *Use the checked in samples for HBase/Storm/Accumulo as reference for script development.*



