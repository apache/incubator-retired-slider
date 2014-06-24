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

How to create a Slider package for HBase?

The version of HBase used for the app package can be adjusted by adding a
flag such as
  -Dhbase.version=0.98.3

Copy the tarball for HBase:
  cp ~/Downloads/hbase-0.98.3-hadoop2-bin.tar.gz package/files/

Use the following command to install HBase tarball locally:
  mvn install:install-file -Dfile=<path-to-tarball> -DgroupId=org.apache.hbase -DartifactId=hbase -Dversion=0.98.3-hadoop2 -Dclassifier=bin -Dpackaging=tar.gz

After HBase tarball is publised locally in maven repository, you can use the following command:
  mvn clean package -DskipTests -Phbase-app-package
App package can be found in
  app-packages/HBase/target/apache-slider-hbase-${hbase.version}-app-package-${slider.version}.zip

Create a zip package at the root of the package (<slider enlistment>/app-packages/hbase/)
  zip -r hbase-v098.zip .

Verify the content using
  zip -Tv apache-slider-hbase-*.zip

While appConfig.json and resources.json are not required for the package they
work well as the default configuration for Slider apps. So it is advisable that
when you create an application package for Slider, include sample/default
resources.json and appConfig.json for a minimal Yarn cluster.

If an HBase version older than 0.98.3 is desired, it must be installed in the
local maven repo.

**Note that the LICENSE.txt and NOTICE.txt that are bundled with the app
package are designed for HBase 0.98.3 only and may need to be modified to be
applicable for other versions of the app package.

A less descriptive file name can be specified with
-Dapp.package.name=HBase_98dot3 which would create a file HBase_98dot3.zip.
