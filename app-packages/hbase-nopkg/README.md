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

# Create Slider App Package for HBase

appConfig-default.json and resources-default.json are not required to be packaged.
These files are included as reference configuration for Slider apps and are suitable
for a one-node cluster.

To create the app package you will need the HBase tarball and invoke mvn command
with appropriate parameters. It is recommended that pkg.version be set to the
 same value as hbase.version.

Command:

    mvn clean package -Phbase-resources -Dhbase.version=<hbase version> -Dpkg.version=<app package version>
       -Dpkg.name=<file name of app tarball> -Dpkg.src=<folder location where the pkg is available>

Example:

    mvn clean package -Phbase-resources -Dhbase.version=1.1.4
      -Dpkg.version=1.1.4 -Dpkg.name=hbase-1.1.4-bin.tar.gz
      -Dpkg.src=/Users/user1/Downloads

App package can be found in

    app-packages/hbase-nopkg/target/slider-hbase-resources-1.1.4.zip

## Verifying the content

Verify the content using

    zip -Tv slider-hbase-*.zip

## Sample commands

    unzip slider-hbase-resources-1.1.4.zip
    slider resource --install --resource resources --destdir hbase
    slider create hbase --template appConfig-default.json --resources resources-default.json --metainfo metainfo.xml
    slider client --install --dest client_install_dir --name hbase --config clientInstallConfig-default.json


