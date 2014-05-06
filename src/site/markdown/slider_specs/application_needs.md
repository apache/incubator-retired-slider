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

# Slider's needs of an application

Slider installs and runs applications in a YARN cluster -applications that
do not need to be written for YARN. 

What they do need to be is deployable by Slider, which means installable by YARN,
configurable by Slider, and, finally, executable by YARN. YARN will kill the
executed process when destroying a container, so the deployed application
must expect this to happen and be able to start up from a kill-initiated
shutdown without any manual recovery process.

They need to locate each other dynamically, both at startup and during execution,
because the location of processes will be unknown at startup, and may change
due to server and process failures. 

## Must

* Install and run from a tarball -and be run from a user that is not root. 

* Be self contained or have all dependencies pre-installed.

* Support dynamic discovery of nodes -such as via ZK.

* Nodes to rebind themselves dynamically -so if nodes are moved, the application
can continue

* Handle kill as a normal shutdown mechanism.

* Support multiple instances of the application running in the same cluster,
  with processes from different application instances sharing
the same servers.

* Operate correctly when more than one role instance in the application is
deployed on the same physical host. (If YARN adds anti-affinity options in 
container requests this will no longer be a requirement)

* Dynamically allocate any RPC or web ports -such as supporting 0 as the number
of the port to listen on  in configuration options.

* Be trusted. YARN does not run code in a sandbox.

* If it talks to HDFS or other parts of Hadoop, be built against/ship with
libaries compatible with the version of Hadoop running on the cluster.

* Store persistent data in HDFS (directly or indirectly) with the exact storage location
configurable. Specifically: not to the local filesystem, and not in a hard coded location
such as `hdfs://app/data`. Slider creates per-instance directories for
persistent data.

* Be configurable as to where any configuration directory is (or simply relative
to the tarball). The application must not require it to be in a hard-coded
location such as `/etc`.

* Not have a fixed location for log output -such as `/var/log/something`

* Run until explicitly terminated. Slider treats an application termination
(which triggers a container release) as a failure -and reacts to it by restarting
the container.



## MUST NOT

* Require human intervention at startup or termination.

## SHOULD

These are the features that we'd like from a service:

* Publish the actual RPC and HTTP ports in a way that can be picked up, such as via ZK
or an admin API.

* Be configurable via the standard Hadoop mechanisms: text files and XML configuration files.
If not, custom parsers/configuration generators will be required.

* Support an explicit parameter to define the configuration directory.

* Take late bindings params via -D args or similar

* Be possible to exec without running a complex script, so that process inheritance works everywhere, including (for testing) OS/X

* Provide a way for Slider to get list of nodes in cluster and status. This will let Slider detect failed worker nodes and react to it.

* FUTURE: If a graceful decommissioning is preferred, have an RPC method that a Slider provider can call to invoke this.

* Be location aware from startup. Example: worker nodes to be allocated tables to serve based on which tables are
stored locally/in-rack, rather than just randomly. This will accelerate startup time.

* Support simple liveness probes (such as an HTTP GET operations).

* Return a well documented set of exit codes, so that failures can be propagated
  and understood.

* Support cluster size flexing: the dynamic addition and removal of nodes.


* Support a management platform such as Apache Ambari -so that the operational
state of a Slider application can be monitored.

## MAY

* Include a single process that will run at a fixed location and whose termination
can trigger application termination. Such a process will be executed
in the same container as the Slider AM, and so known before all other containers
are requested. If a live cluster is unable to handle restart/migration of 
such a process, then the Slider application will be unable to handle
Slider AM restarts.

* Ideally: report on load/cost of decommissioning.
  E.g amount of data; app load. 


## MAY NOT

* Be written for YARN.

* Be (pure) Java. If the tarball contains native binaries for the cluster's hardware & OS,
  they should be executable.

* Be dynamically reconfigurable, except for the special requirement of handling
movement of manager/peer containers in an application-specific manner.


