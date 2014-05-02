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

#Application Configuration

App Configuration captures the default configuration associated with the application. *Details of configuration management is discussed in a separate spec*. The default configuration is modified based on user provided InstanceConfiguration, cluster specific details (e.g. HDFS root, local dir root), container allocated resources (port and hostname), and dependencies (e.g. ZK quorom hosts) and handed to the component instances.

App Configuration is a folder containing all configuration needed by the application. Config files include any site.xml, log4j properties file, etc. 

In addition, application may have configuration parameters that do not necessarily go into a config files. Such configurations may be used during template expansion (parameters in env.sh files), as environment variables (e.g. JAVA_HOME), customize user names (for runas). These configurations can be provided as user inputs or are automatically inferred from the environment. Such configurations are stored in a file named "app_config.xml".

![Image](../images/app_config_folders_01.png?raw=true)

A config file is of the form:

```
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
  ...
  </property>
</configuration>
```


Each configuration property is specified as follows:

```
<property>
    <name>storm.zookeeper.session.timeout</name>
    <value>20000</value>
    <description>The session timeout for clients to ZooKeeper.</description>
    <required>false</required>
    <valueRestriction>0-30000</valueRestriction>
  </property>
  <property>
    <name>storm.zookeeper.root</name>
    <value>/storm</value>
    <description>The root location at which Storm stores data in ZK.</description>
    <required>true</required>
  </property>
  <property>
    <name>jvm.heapsize</name>
    <value>256</value>
    <description>The default JVM heap size for any component instance.</description>
    <required>true</required>
  </property>
  <property>
    <name>nimbus.host</name>
    <value>localhost</value>
    <description>The host that the master server is running on.</description>
    <required>true</required>
    <clientVisible>true</clientVisible>
  </property>
  ```


* name: name of the parameter

* value: default value of the parameter

* description: a short description of the parameter

* required: if the parameter is mandatory in which case it must have a value - default is "false"

* clientVisible: if the property must be exported for a client

