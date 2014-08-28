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


slider_setup
============

Tools for installing, starting, and destroying HBase, Accumulo, and Storm slider apps on YARN.

**WARNING: This is intended for POC/sandbox testing, may not be idempotent so DO NOT use on an existing Production cluster!!!**

Setup
-----
1. Clone the repo
2. Set the necessary cluster variables in `slider_setup.conf`, it shouldn't be necessary to change any other values but thoe ones below


    # Zookeeper nodes
    ZK_QUORUM="zk1:2181,zk2:2181,zk3:2181"
    
    # Resource Manager address (yarn.resourcemanager.address)
    RM_ADDRESS="rm1:8050"
    
    # Resource Manager scheduler address (yarn.resourcemanager.scheduler.address)
    RM_SCHED_ADDRESS="rm1:8030"
    
    # Default FS (fs.defaultFS)
    DEFAULT_FS="hdfs://nn1:8020"

Running
-------
* slider_setup is the main script and handles the following
  1. Pulls down slider and extracts the contents to the SLIDER_INST_DIR
  2. Modifies slider-client.xml with cluster related info
  3. Pulls down the slider enabled version of the specified product
  4. Creates necessary directories and copies required files to HDFS
  5. For HBase, creates the app dir in HDFS
  6. Submits the slider base application to the YARN cluster

* The following args are required
  * -f - The path to the slider_setup.conf that has been modified with cluster info
  * -p - The product to run (hbase, accumulo, or storm are all that are supported at this time)
  * -w - The number of "worker" nodes. This has different meaning depending on product.
    * HBase - number of region servers
    * Accumulo - number of tablet servers
    * Storm - number of supervisors
  * -n - The name of the app, this will be the display name in the resource manager and is used by the teardown process

* HBase Example:


    ./slider_setup -f slider_setup.conf -p hbase -w 5 -n hbase-slider

* Accumulo Example:


    ./slider_setup -f slider_setup.conf -p accumulo -w 3 -n accumulo-slider

* Storm Example:


    ./slider_setup -f slider_setup.conf -p storm -w 3 -n storm-slider

Tear Down
---------

* slider_destroy will do the following
  1. Stop the slider application based on provided name
  2. Destory the slider application based on provided name

* The following args are required
  * `-f` - The path to the `slider_setup.conf` that has been modified with cluster info
  * `-n` - The name of the app, this was provided to the slider_setup tool

* HBase Example:


    ./slider_destroy -f slider_setup.conf -n hbase-slider

* Accumulo Example:


    ./slider_destroy -f slider_setup.conf -n accumulo-slider

* Storm Example:


    ./slider_destroy -f slider_setup.conf -n storm-slider
