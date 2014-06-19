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

How to create a Slider package for Accumulo?

  mvn clean package -DskipTests -Paccumulo-app-package

App package can be found in
  app-packages/accumulo/target/apache-slider-accumulo-${accumulo.version}-app-package-${slider.version}.zip

Verify the content using
  zip -Tv apache-slider-accumulo-*.zip

While appConfig.json and resources.json are not required for the package they
work well as the default configuration for Slider apps. So it is advisable that
when you create an application package for Slider, include sample/default
resources.json and appConfig.json for a minimal Yarn cluster.

The version of Accumulo used for the app package can be adjusted by adding a
flag such as
  -Daccumulo.version=1.6.1

If an Accumulo version older than 1.6.0 is desired, it must be installed in the
local maven repo, e.g.
  mvn install:install-file -Dfile=~/Downloads/accumulo-1.5.1-bin.tar.gz -DgroupId=org.apache.accumulo -DartifactId=accumulo -Dversion=1.5.1 -Dclassifier=bin -Dpackaging=tar.gz

**Note that the LICENSE.txt and NOTICE.txt that are bundled with the app
package are designed for Accumulo 1.6.0 only and may need to be modified to be
applicable for other versions of the app package.

Note also that the sample appConfig.json provided only works with Accumulo 1.6,
while for Accumulo 1.5 the instance.volumes property must be replaced with
instance.dfs.dir (and it cannot use the provided variable ${DEFAULT_DATA_DIR}
which is an HDFS URI).

A less descriptive file name can be specified with
-Dapp.package.name=accumulo_160 which would create a file accumulo_160.zip.
