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

## Build

Check out the appropriate branch of slider, and build the project, providing the
necessary profile activation for the Tomcat app package. You probably also
want to skip tests. This will place zip file in `app-packages/tomcat/target`
that will be used later.

`mvn clean package -DskipTests -Ptomcat-app-package`

## Installation

The Tomcat application package is distributed as a zip archive.
Use the `slider install` command to install it into HDFS for use.
You must also provide an identifying name (such as "tomcat").

`slider package --install --name tomcat --package path/to/slider-tomcat-app-package-0.91.0-incubating-SNAPSHOT.zip`

## Configuration

Copy appConfig-default.json and resources-default.json. By default, these files
will create one instance of Tomcat using a dynamic port provided by Slider.

In the copied appConfig.json, make sure you set `application.def` and
`java_home` to the correct values. Feel free to adjust the memory usage as well.


## Create a Slider cluster

Use the appConfig.json and resources.json file to create a Slider cluster now.
The name of the cluster is user defined and must be unique: this example uses "tomcat1"

`slider create tomcat1 --template path/to/appConfig.json --resources path/to/resources.json`

## Verify

Check the Slider Application Master page on the Hadoop YARN status page. You should see
the application running and without failures. The address of the Tomcat servers will be listed
as exports on the AppMaster's web UI.
