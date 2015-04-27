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

# Apache Tomcat

Basic instructions on using the Apache Tomcat app-package.

## Installation

The Tomcat application package is distributed as a zip archive.
Use the `slider install` command to install it into HDFS for use.
You must also provide an identifying name (such as "tomcat").

`slider install-package --name tomcat --package path/to/slider-tomcat-app-package-0.72.0-incubating-SNAPSHOT.zip`

## Configuration

Copy appConfig-default.json and resources-default.json. By default, these files
will create one instance of Tomcat using a dynamic port provided by
Slider.

In the copied appConfig.json, make sure you set `application.def` and
`java_home`. Feel free to adjust the memory usage as well.


## Create a Slider cluster

Use the appConfig.json and resources.json file to create a Slider cluster now.
The name of the cluster is user defined and must be unique: this example uses "tomcat1"

`slider create tomcat1 --template path/to/appConfig.json --resources path/to/resources.json`

## Verify

Check the Slider Application Master page on the Hadoop YARN status page. You should see
the application running and without failures.

Until the QuickLinks/exports is resolved for this app package, the port must be determined manually
using a tool such as `netstat` and the pid of the Tomcat process. Sorry.
