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

# Application Definition

App definition is a declarative definition of a YARN application describing its content. The AppDefinition is used in conjunction with the [AppPackage](application_package.md).

## Structure

*Non-mandatory fields are described in **italics**.*

The fields to describe an application is as follows:

* **name**: the name of the application

* **version**: the version of the application. name and version together uniquely identify an application.

* **type**: the type of the application. "YARN-APP" identifies an application definition suitable for YARN.

* **minHadoopVersion**: the minimum version of hadoop on which the app can run

* **components**: the list of component that the application is comprised of

* **osSpecifics**: OS specific package information for the application

* *commandScript*: application wide commands may also be defined. The command is executed on a component instance that is a client

* *dependencies*: application can define a list of dependencies. Dependencies can be on the base services such as HDFS, ZOOKEEPER, YARN which are infrastructure services or GANGLIA, NAGIOS, etc. which are monitoring/alert services. The dependencies are parsed by the management infrastructure to provide the necessary configurations to allow the app to access the services. For example, a HDFS folder could be requested by the app to store its data, a ZOOKEEPER node to co-ordinate among components.

An application contains several component. The fields associated with a component are:

* **name**: name of the component

* **category**: type of the component - MASTER, SLAVE, and CLIENT

* **minInstanceCount**: the minimum number of instances required for this component

* *maxInstanceCount*: maximum number of instances allowed for a component

* **commandScript**: the script that implements the commands.

 * **script**: the script location - relative to the AppPackage root

 * **scriptType**: type of the script

 * **timeout**: default timeout of the script

* *customCommands*: any additional commands available for the component and their implementation

An application definition also includes the package used to install the application. Its typically a tarball or some other form of package that does not require root access to install. The details of what happens during install is captured in the command script.

* **osSpecific**: details on a per OS basis

* **osType**: "any" refers to any OS ~ typical for tarballs

* **packages**: list of packages that needs to be deployed

* **type**: type of package

* **name**: name of the package

* **location**: location of the package (can be a relative folder within the parent AppPackage)

Application can define a set of dependencies. The dependencies are parsed by Slider to provide additional configuration parameters to the command scripts. For example, an application may need to ZooKeeper quorum hosts to communicate with ZooKeeper. In this case, ZooKeeper is a "base" service available in the cluster.

* **dependency**: an application can specify more than one dependency

* **name**: a well-known name of the base service (or another application) on which the dependency is defined

* **scope**: is the dependent service/application expected on the same cluster or it needs to be on the same hosts where components are instantiated

* **requirement**: a set of requirements that lets Slider know what properties are required by the app command scripts

```
  <metainfo>
    <schemaVersion>2.0</schemaVersion>
    <application>
      <name>HBASE</name>
      <version>0.96.0.2.1.1</version>
      <type>YARN-APP</type>
      <minHadoopVersion>2.1.0</minHadoopVersion>
      <components>
        <component>
          <name>HBASE_MASTER</name>
          <category>MASTER</category>
          <minInstanceCount>1</minInstanceCount>
          <maxInstanceCount>2</maxInstanceCount>
          <commandScript>
            <script>scripts/hbase_master.py</script>
            <scriptType>PYTHON</scriptType>
            <timeout>600</timeout>
          </commandScript>
          <customCommands>
            <customCommand>
              <name>GRACEFUL_STOP</name>
              <commandScript>
                <script>scripts/hbase_master.py</script>
                <scriptType>PYTHON</scriptType>
                <timeout>1800</timeout>
              </commandScript>
          </customCommand>
        </customCommands>
        </component>

        <component>
          <name>HBASE_REGIONSERVER</name>
          <category>SLAVE</category>
          <minInstanceCount>1</minInstanceCount>
          ...
        </component>

        <component>
          <name>HBASE_CLIENT</name>
          <category>CLIENT</category>
          ...
      </components>

      <osSpecifics>
        <osSpecific>
          <osType>any</osType>
          <packages>
            <package>
              <type>tarball</type>
              <name>hbase-0.96.1-tar.gz</name>
              <location>package/files</location>
            </package>
          </packages>
        </osSpecific>
      </osSpecifics>

      <commandScript>
        <script>scripts/app_health_check.py</script>
        <scriptType>PYTHON</scriptType>
        <timeout>300</timeout>
      </commandScript>

      <dependencies>
        <dependency>
          <name>ZOOKEEPER</name>
          <scope>cluster</scope>
          <requirement>client,zk_quorom_hosts</requirement>
        </dependency>
      </dependencies>

    </application>
  </metainfo>
```


## Open Questions

1. Applications may need some information from other applications or base services such as ZK, YARN, HDFS. Additionally, they may need a dedicated ZK node, a HDFS working folder, etc. How do we capture this requirement? There needs to be a well-known way to ask for these information e.g. fs.default.name, zk_hosts.

2. Similar to the above there are common parameters such as JAVA_HOME and other environment variables. Application should be able to refer to these parameters and Slider should be able to provide them.

3. Composite application definition: Composite application definition would require a spec that refers to this spec and binds multiple applications together.

