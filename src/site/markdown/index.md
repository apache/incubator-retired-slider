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


# Slider: Dynamic YARN Applicatins



Slider is a YARN application to deploy existing distributed applications on YARN, 
monitor them and make them larger or smaller as desired -even while 
the cluster is running.


Slider has a plug-in *provider* architecture to support different applications,
and currently supports Apache HBase and Apache Accumulo.

Clusters can be stopped, "frozen" and restarted, "thawed" later; the distribution
of the deployed application across the YARN cluster is persisted -enabling
a best-effort placement close to the previous locations on a cluster thaw.
Applications which remember the previous placement of data (such as HBase)
can exhibit fast start-up times from this feature.

YARN itself monitors the health of 'YARN containers" hosting parts of 
the deployed application -it notifies the Slider manager application of container
failure. Slider then asks YARN for a new container, into which Slider deploys
a replacement for the failed component. As a result, Slider can keep the
size of managed applications consistent with the specified configuration, even
in the face of failures of servers in the cluster -as well as parts of the
application itself

Some of the features are:

* Allows users to create on-demand Apache HBase and Apache Accumulo clusters

* Allow different users/applicatins to run different versions of the application.

* Allow users to configure different Hbase instances differently

* Stop / Suspend / Resume clusters as needed

* Expand / shrink clusters as needed

The Slider tool is a Java command line application.

The tool persists the information as a JSON document into the HDFS.
It also generates the configuration files assuming the passed configuration
directory as a base - in particular, the HDFS and ZooKeeper root directories
for the new HBase instance has to be generated (note that the underlying
HDFS and ZooKeeper are shared by multiple cluster instances). Once the
cluster has been started, the cluster can be made to grow or shrink
using the Slider commands. The cluster can also be stopped, *frozen*
and later resumed, *thawed*.
      
Slider implements all its functionality through YARN APIs and the existing
application shell scripts. The goal of the application was to have minimal
code changes and as of this writing, it has required few changes.

## Using 

* [Announcement](announcement.html)
* [Installing](installing.html)
* [Man Page](manpage.html)
* [Examples](examples.html)
* [Client Configuration](hoya-client-configuration.html)
* [Client Exit Codes](exitcodes.html)
* [Cluster Descriptions](hoya_cluster_descriptions.html)
* [Security](security.html)
* [Logging](logging.html)

## Developing 

* [Architecture](architecture.html)
* [Application Needs](app_needs.html)
* [Building](building.html)
* [Releasing](releasing.html)
* [Role history](rolehistory.html) 
* [Specification](specification/index.html)
* [Application configuration model](configuration/index.html)
