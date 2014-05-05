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

# Project Slider: Getting Started


## Introduction

The following provides the steps required for setting up a cluster and deploying a YARN hosted application using Slider.

* [Prerequisites](#sysreqs)

* [Setup the Cluster](#setup)

* [Download Slider Packages](#download)

* [Build Slider](#build)

* [Install Slider](#install)

* [Deploy Slider Resources](#deploy)

* [Download Sample Application Packages](#downsample)

* [Install, Configure, Start and Verify Sample Application](#installapp)

* [Appendix A: Storm Sample Application Specifications](#appendixa)

* [Appendix B: HBase Sample Application Specifications](#appendixb)

## <a name="sysreqs"></a>System Requirements

The Slider deployment has the following minimum system requirements:

* Hortonworks Data Platform 2.1

* Required Services: HDFS, YARN, MapReduce2 and ZooKeeper

* Oracle JDK 1.7 (64-bit)

## <a name="setup"></a>Setup the Cluster

After [installing your cluster](http://docs.hortonworks.com/) (using Ambari or other means) with the Services listed above, modify your YARN configuration to allow for multiple containers on a single host. In yarn-site.xml make the following modifications:

<table>
  <tr>
    <td>Property</td>
    <td>Value</td>
  </tr>
  <tr>
    <td>yarn.scheduler.minimum-allocation-mb</td>
    <td>>= 256</td>
  </tr>
  <tr>
    <td>yarn.nodemanager.delete.debug-delay-sec</td>
    <td>>= 3600 (to retain for an hour)</td>
  </tr>
</table>


There are other options detailed in the Troubleshooting file available <a href="troubleshooting.html">here</a>


## <a name="download"></a>Download Slider Packages

The sample application packages for Storm, HBase and Accumulo are available at:

[http://public-repo-1.hortonworks.com/slider/slider-0.22.0-all.tar.gz](http://public-repo-1.hortonworks.com/slider/slider-0.22.0-all.tar.gz)
## <a name="build"></a>Build Slider

* From the top level directory, execute "mvn clean install -DskipTests"
* Use the generated compressed tar file in slider-assembly/target directory (e.g. slider-0.22.0-all.tar.gz) for the subsequent steps

## <a name="install"></a>Install Slider

Follow the following steps to expand/install Slider:

* mkdir *slider-install-dir*;

* cd *slider-install-dir*

* Login as the ‘yarn’ user (assuming this is a host associated with the installed cluster).  E.g., su yarn
*This assumes that all apps are being run as ‘yarn’ user. Any other user can be used to run the apps - ensure that file permission is granted as required.*

* Expand the tar file:  tar -xvf slider-0.22.0-all.tar.gz

* Browse to the Slider directory: cd slider-0.22.0/bin

* export PATH=$PATH:/usr/jdk64/jdk1.7.0_45/bin (or the path to the JDK bin directory)

* Modify Slider configuration file *slider-install-dir*/slider-0.22.0/conf/slider-client.xml to add the following properties:

```
		<property>
  			<name>yarn.application.classpath</name>
  			<value>/etc/hadoop/conf,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop-mapreduce/lib/*</value>
		</property>
		<property>
  			<name>slider.zookeeper.quorum</name>
  			<value>yourZooKeeperHost:port</value>
		</property>
```

In addition, specify the scheduler and HDFS addresses as follows:

```
		<property>
  			<name>yarn.resourcemanager.address</name>
  			<value>yourResourceManagerHost:8050</value>
		</property>
		<property>
  			<name>yarn.resourcemanager.scheduler.address</name>
  			<value>yourResourceManagerHost:8030</value>
		</property>
		<property>
  			<name>fs.defaultFS</name>
  			<value>hdfs://yourNameNodeHost:8020</value>
		</property>
```


* Execute: *slider-install-dir*/slider-0.22.0/bin/slider version

* Ensure there are no errors and you can see "Compiled against Hadoop 2.4.0"

## <a name="deploy"></a>Deploy Slider Resources

Ensure that all file folders are accessible to the user creating the application instance. The example assumes "yarn" to be that user.

### Create HDFS root folder for Slider

Perform the following steps to create the Slider root folder with the appropriate permissions:

* su hdfs

* hdfs dfs -mkdir /slider

* hdfs dfs -chown yarn:hdfs /slider

* hdfs dfs -mkdir /user/yarn

* hdfs dfs -chown yarn:hdfs /user/yarn

### Load Slider Agent

* su yarn

* hdfs dfs -mkdir /slider/agent

* hdfs dfs -mkdir /slider/agent/conf

* hdfs dfs -copyFromLocal *slider-install-dir*/slider-0.22.0/agent/slider-agent-0.22.0.tar.gz /slider/agent

### Create and deploy Slider Agent configuration

Create an agent config file (agent.ini) based on the sample available at:

*slider-install-dir*/slider-0.22.0/agent/conf/agent.ini

The sample agent.ini file can be used as is (see below). Some of the parameters of interest are:

* log_level = INFO or DEBUG, to control the verbosity of log

* app_log_dir = the relative location of the application log file

* log_dir = the relative location of the agent and command log file

		[server]
		hostname=localhost
		port=8440
		secured_port=8441
		check_path=/ws/v1/slider/agents/
		register_path=/ws/v1/slider/agents/{name}/register
		heartbeat_path=/ws/v1/slider/agents/{name}/heartbeat

		[agent]
		app_pkg_dir=app/definition
		app_install_dir=app/install
		app_run_dir=app/run
		app_task_dir=app/command-log
		app_log_dir=app/log
		app_tmp_dir=app/tmp
		log_dir=infra/log
		run_dir=infra/run
		version_file=infra/version
		log_level=INFO

		[python]

		[command]
		max_retries=2
		sleep_between_retries=1

		[security]

		[heartbeat]
		state_interval=6
		log_lines_count=300


Once created, deploy the agent.ini file to HDFS:

* su yarn

* hdfs dfs -copyFromLocal agent.ini /slider/agent/conf

## <a name="downsample"></a>Download Sample Application Packages

There are three sample application packages available for download to use with Slider:

<table>
  <tr>
    <td>Application</td>
    <td>Version</td>
    <td>URL</td>
  </tr>
  <tr>
    <td>Apache HBase</td>
    <td>0.96.0</td>
    <td>http://public-repo-1.hortonworks.com/slider/hbase_v096.tar</td>
  </tr>
  <tr>
    <td>Apache Storm</td>
    <td>0.9.1</td>
    <td>http://public-repo-1.hortonworks.com/slider/storm_v091.tar</td>
  </tr>
  <tr>
    <td>Apache Accumulo</td>
    <td>1.5.1</td>
    <td>http://public-repo-1.hortonworks.com/slider/accumulo_v151.tar</td>
  </tr>
</table>


Download the packages and deploy one of these sample applications to YARN via Slider using the steps below.

## <a name="installapp"></a>Install, Configure, Start and Verify Sample Application

* [Load Sample Application Package](#load)

* [Create Application Specifications](#create)

* [Start the Application](#start)

* [Verify the Application](#verify)

* [Manage the Application Lifecycle](#manage)

### <a name="load"></a>Load Sample Application Package

* hdfs dfs -copyFromLocal *sample-application-package* /slider

If necessary, create HDFS folders needed by the application. For example, HBase requires the following HDFS-based setup:

* su hdfs

* hdfs dfs -mkdir /apps

* hdfs dfs -mkdir /apps/hbase

* hdfs dfs -chown yarn:hdfs /apps/hbase

### <a name="create"></a>Create Application Specifications

Configuring a Slider application consists of two parts: the *[Resource Specification](#resspec), and the *[Application Configuration](#appconfig). Below are guidelines for creating these files.

*Note: There are sample Resource Specifications (**resources.json**) and Application Configuration (**appConfig.json**) files in the *[Appendix](#appendixa)* and also in the root directory of the Sample Applications packages (e.g. /**hbase-v096/resources.json** and /**hbase-v096/appConfig.json**).*

#### <a name="resspec"></a>Resource Specification

Slider needs to know what components (and how many components) are in an application package to deploy. For example, in HBase, the components are **_master_** and **_worker_** -- the latter hosting **HBase RegionServers**, and the former hosting the **HBase Master**. 

As Slider creates each instance of a component in its own YARN container, it also needs to know what to ask YARN for in terms of **memory** and **CPU** for those containers. 

All this information goes into the **Resources Specification** file ("Resource Spec") named resources.json. The Resource Spec tells Slider how many instances of each component in the application (such as an HBase RegionServer) to deploy and the parameters for YARN.

Sample Resource Spec files are available in the Appendix:

* [Appendix A: Storm Sample Resource Specification](#heading=h.1hj8hn5xne7c)

* [Appendix B: HBase Sample Resource Specification](#heading=h.l7z5mvhvxmzv)

Store the Resource Spec file on your local disk (e.g. /tmp/resources.json).

#### <a name="appconfig"></a>Application Configuration

Alongside the Resource Spec there is the **Application Configuration** file ("App Config") which includes parameters that are specific to the application, rather than YARN. The App Config is a file that contains all application configuration. This configuration is applied to the default configuration provided by the application definition and then handed off to the associated component agent.

For example, the heap sizes of the JVMs,  The App Config defines the configuration details **specific to the application and component** instances. For HBase, this includes any values for the *to-be-generated *hbase-site.xml file, as well as options for individual components, such as their heap size.

Sample App Configs are available in the Appendix:

* [Appendix A: Storm Sample Application Configuration](#heading=h.2qai3c6w260l)

* [Appendix B: HBase Sample Application Configuration](#heading=h.hetv1wn44c5x)

Store the appConfig.json file on your local disc and a copy in HDFS:

* su yarn

* hdfs dfs -mkdir /slider/appconf

* hdfs dfs -copyFromLocal appConf.json /slider/appconf

### <a name="start"></a>Start the Application

Once the steps above are completed, the application can be started by leveraging the **Slider Command Line Interface (CLI)**.

* Change directory to the "bin" directory under the slider installation

cd *slider-install-dir*/slider-0.22.0/bin

* Execute the following command:

./slider create cl1 --manager yourResourceManagerHost:8050 --image hdfs://yourNameNodeHost:8020/slider/agent/slider-agent-0.22.0.tar.gz --template appConfig.json --resources resources.json

### <a name="verify"></a>Verify the Application

The successful launch of the application can be verified via the YARN Resource Manager Web UI. In most instances, this UI is accessible via a web browser at port 8088 of the Resource Manager Host:

![image alt text](images/image_0.png)

The specific information for the running application is accessible via the "ApplicationMaster" link that can be seen in the far right column of the row associated with the running application (probably the top row):

![image alt text](images/image_1.png)

### <a name="manage"></a>Manage the Application Lifecycle

Once started, applications can be frozen/stopped, thawed/restarted, and destroyed/removed as follows:

#### Frozen:

./slider freeze cl1 --manager yourResourceManagerHost:8050  --filesystem hdfs://yourNameNodeHost:8020

#### Thawed: 

./slider thaw cl1 --manager yourResourceManagerHost:8050  --filesystem hdfs://yourNameNodeHost:8020

#### Destroyed: 

./slider destroy cl1 --manager yourResourceManagerHost:8050  --filesystem hdfs://yourNameNodeHost:8020

#### Flexed:

./slider flex cl1 --component worker 5 --manager yourResourceManagerHost:8050  --filesystem hdfs://yourNameNodeHost:8020

# <a name="appendixa"></a>Appendix A: Apache Storm Sample Application Specifications

## Storm Resource Specification Sample

	{
      "schema" : "http://example.org/specification/v2.0.0",
      "metadata" : {
      },
      "global" : {
      },
      "components" : {
        "slider-appmaster" : {
        },
        "NIMBUS" : {
            "role.priority" : "1",
            "component.instances" : "1"
        },
        "STORM_REST_API" : {
            "wait.heartbeat" : "3",
            "role.priority" : "2",
            "component.instances" : "1"
        },
        "STORM_UI_SERVER" : {
            "role.priority" : "3",
            "component.instances" : "1"
        },
        "DRPC_SERVER" : {
            "role.priority" : "4",
            "component.instances" : "1"
        },
        "SUPERVISOR" : {
            "role.priority" : "5",
            "component.instances" : "1"
        }
      }
	}


## Storm Application Configuration Sample

	{
    	"schema" : "http://example.org/specification/v2.0.0",
    	"metadata" : {
    	},
    	"global" : {
        	"A site property for type XYZ with name AA": "its value",
        	"site.XYZ.AA": "Value",
        	"site.hbase-site.hbase.regionserver.port": "0",
        	"site.core-site.fs.defaultFS": "${NN_URI}",
        	"Using a well known keyword": "Such as NN_HOST for name node host",
        	"site.hdfs-site.dfs.namenode.http-address": "${NN_HOST}:50070",
        	"a global property used by app scripts": "not affiliated with any site-xml",
        	"site.global.app_user": "yarn",
        	"Another example of available keywords": "Such as AGENT_LOG_ROOT",
        	"site.global.app_log_dir": "${AGENT_LOG_ROOT}/app/log",
        	"site.global.app_pid_dir": "${AGENT_WORK_ROOT}/app/run",
    	}
	}


# <a name="appendixb"></a>Appendix B:  Apache HBase Sample Application Specifications

## HBase Resource Specification Sample

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


## HBase Application Configuration Sample

	{
      "schema" : "http://example.org/specification/v2.0.0",
      "metadata" : {
      },
      "global" : {
        "agent.conf": "/slider/agent/conf/agent.ini",
        "agent.version": "/slider/agent/version",
        "application.def": "/slider/hbase_v096.tar",
        "config_types": "core-site,hdfs-site,hbase-site",
        "java_home": "/usr/jdk64/jdk1.7.0_45",
        "package_list": "files/hbase-0.96.1-hadoop2-bin.tar",
        "site.global.app_user": "yarn",
        "site.global.app_log_dir": "${AGENT_LOG_ROOT}/app/log",
        "site.global.app_pid_dir": "${AGENT_WORK_ROOT}/app/run",
        "site.global.app_root": "${AGENT_WORK_ROOT}/app/install/hbase-0.96.1-hadoop2",
        "site.global.app_install_dir": "${AGENT_WORK_ROOT}/app/install",
        "site.global.hbase_master_heapsize": "1024m",
        "site.global.hbase_regionserver_heapsize": "1024m",
        "site.global.user_group": "hadoop",
        "site.global.security_enabled": "false",
        "site.hbase-site.hbase.hstore.flush.retries.number": "120",
        "site.hbase-site.hbase.client.keyvalue.maxsize": "10485760",
        "site.hbase-site.hbase.hstore.compactionThreshold": "3",
        "site.hbase-site.hbase.rootdir": "${NN_URI}/apps/hbase/data",
        "site.hbase-site.hbase.stagingdir": "${NN_URI}/apps/hbase/staging",
        "site.hbase-site.hbase.regionserver.handler.count": "60",
        "site.hbase-site.hbase.regionserver.global.memstore.lowerLimit": "0.38",
        "site.hbase-site.hbase.hregion.memstore.block.multiplier": "2",
        "site.hbase-site.hbase.hregion.memstore.flush.size": "134217728",
        "site.hbase-site.hbase.superuser": "yarn",
        "site.hbase-site.hbase.zookeeper.property.clientPort": "2181",
        "site.hbase-site.hbase.regionserver.global.memstore.upperLimit": "0.4",
        "site.hbase-site.zookeeper.session.timeout": "30000",
        "site.hbase-site.hbase.tmp.dir": "${AGENT_WORK_ROOT}/work/app/tmp",
        "site.hbase-site.hbase.local.dir": "${hbase.tmp.dir}/local",
        "site.hbase-site.hbase.hregion.max.filesize": "10737418240",
        "site.hbase-site.hfile.block.cache.size": "0.40",
        "site.hbase-site.hbase.security.authentication": "simple",
        "site.hbase-site.hbase.defaults.for.version.skip": "true",
        "site.hbase-site.hbase.zookeeper.quorum": "${ZK_HOST}",
        "site.hbase-site.zookeeper.znode.parent": "/hbase-unsecure",
        "site.hbase-site.hbase.hstore.blockingStoreFiles": "10",
        "site.hbase-site.hbase.hregion.majorcompaction": "86400000",
        "site.hbase-site.hbase.security.authorization": "false",
        "site.hbase-site.hbase.cluster.distributed": "true",
        "site.hbase-site.hbase.hregion.memstore.mslab.enabled": "true",
        "site.hbase-site.hbase.client.scanner.caching": "100",
        "site.hbase-site.hbase.zookeeper.useMulti": "true",
        "site.hbase-site.hbase.regionserver.info.port": "0",
        "site.hbase-site.hbase.master.info.port": "60010",
        "site.hbase-site.hbase.regionserver.port": "0",
        "site.core-site.fs.defaultFS": "${NN_URI}",
        "site.hdfs-site.dfs.namenode.https-address": "${NN_HOST}:50470",
        "site.hdfs-site.dfs.namenode.http-address": "${NN_HOST}:50070"
      }
	}


