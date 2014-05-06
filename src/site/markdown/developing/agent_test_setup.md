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

# Setting up for Agent test

     The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL
      NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and
      "OPTIONAL" in this document are to be interpreted as described in
      RFC 2119.

An agent is an application independent entity that can handle any application as long as its definition conforms to a specific format (TBD). While Slider will install the agent in its entirity, for now the host, where the agent is deployed, needs to be prepared for the agent. The following steps are necessary to setup the host for agent.

## Install resource management library
The resource management library is a common python library used by all application specification. The library is either pre-installed on the hosts or it is packaged along with the application package itself.

Get resource_management.tar and install it at /usr/lib/python2.6/site-packages

## Install the application spec
The application spec is the declarative definition of a application that can be run on YARN. *At this point only HBase application is supported.*

Get HDP-2.0.6.tar and install at /var/lib/ambari-agent/cache/stacks/HDP.

## Install HBase using tarball
Get the hbase tarball, hbase-0.96.1-hadoop2-bin.tar.tar.tar.gz, and expand it at /share/hbase.

## Permissions
Ensure that the user creating the hbase cluster has necessary permission for the resource management library and the application spec. Perform necessary **chown** and **chmod**.

1. /share/hbase/hbase-0.96.1-hadoop2/conf
2. /usr/lib/python2.6/site-packages/resource_management
3. /var/lib/ambari-agent/cache/stacks/HDP
4. /var/log/hbase, /var/run/hbase (or appropriate log and run directories)
5. Ensure hbase root/staging HDFS directories have appropriate permission
