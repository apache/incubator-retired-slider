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

Create Slider App Package for HBase on Windows

appConfig-default.json and resources-default.json are not required to be packaged.
These files are included as reference configuration for Slider apps and are suitable
for a one-node cluster.

To create the app package you will need the HBase tarball and invoke mvn command
with appropriate parameters.

Note: Post 0.94 versions of hbase, tarball filenames have a -bin suffix (e.g.
      hbase-0.98.10.1-hadoop2-bin.tar.gz), although the untarred top level
      directory does not (e.g hbase-0.98.10.1-hadoop2). We suggest renaming
      the tar ball file to remove the -bin suffix before proceeding (e.g.
      hbase-0.98.10.1-hadoop2.tar.gz). In fact the mvn command below assumes
      you have done so, or else app create will fail.

Command:
mvn clean package -Phbase-app-package-win -Dpkg.version=<version>
   -Dpkg.name=<file name of app zip file> -Dpkg.src=<folder location where the pkg is available>

Example:
mvn clean package -Phbase-app-package-win -Dpkg.version=0.98.5-hadoop2
  -Dpkg.name=hbase-0.98.5-hadoop2.zip
  -Dpkg.src=/Users/user1/Downloads

App package can be found in
  app-packages/hbase/target/slider-hbase-app-win-package-${pkg.version}.zip
