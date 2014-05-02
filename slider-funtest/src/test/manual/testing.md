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
  
# Manual Testing

Manual testing invloves using Slider package and an AppPackage to perform basic cluster functionalities such as create/destroy, flex up/down, and freeze/thaw. A python helper script is provided that can be used to automatically test and app package.

## SliderTester.py
Details to be added.

## SliderTester.ini
The various config parameters are:

* **slider**
  * *package:* location of the slider package
  * *jdk.path:* jdk path on the test hosts

* **app**
  * *package:* location of the app package

* **cluster**
  * *yarn.application.classpath:* yarn application classpaths
  * *slider.zookeeper.quorum:* the ZK quorum hosts
  * *yarn.resourcemanager.address:*
  * *yarn.resourcemanager.scheduler.address:*
  * *fs.defaultFS:* e.g. hdfs://NN_HOST:8020

* **test**
  * *app.user:* user to use for app creation
  * *hdfs.root.user:* hdfs root user
  * *hdfs.root.dir:* HDFS root, default /slidertst
  * *hdfs.user.dir:* HDFS user dir, default /user
  * *test.root:* local test root folder, default /test
  * *cluster.name:* name of the test cluster, default tst1
  * *cluster.type:* cluster type to build and test, e.g. hbase,storm,accumulo

* **agent**
