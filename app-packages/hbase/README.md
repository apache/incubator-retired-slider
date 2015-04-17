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

OPTION-I: Use a downloaded tarball
OPTION-II: Use the tarball from the local repository

Note: Post 0.94 versions of hbase, tarball filenames have a -bin suffix (e.g.
      hbase-0.98.10.1-hadoop2-bin.tar.gz), although the untarred top level
      directory does not (e.g hbase-0.98.10.1-hadoop2). We suggest renaming
      the tar ball file to remove the -bin suffix before proceeding with either
      of the below options (e.g. hbase-0.98.10.1-hadoop2.tar.gz). In fact all the
      mvn commands below assumes you have done so, or else app create will fail.

## OPTION - I 

To create the app package you will need the HBase tarball and invoke mvn command
with appropriate parameters.

Command:

    mvn clean package -Phbase-app-package -Dhbase.version=<version>
       -Dpkg.name=<file name of app tarball> -Dpkg.src=<folder location where the pkg is available>

Example:

    mvn clean package -Phbase-app-package -Dhbase.version=0.98.5-hadoop2
      -Dhbase.version=hbase-0.98.5-hadoop2.tar.gz
      -Dpkg.src=/Users/user1/Downloads/0.98.5-hadoop2

App package can be found in

    app-packages/hbase/target/slider-hbase-app-package-${slider.version}.zip

## OPTION - II 

You need the HBase version available on local maven repo to create the Slider App Package for HBase.

Download the tarball for HBase:
  e.g. path to tarball `~/Downloads/hbase-0.98.3-hadoop2.tar.gz`

Note for Phoenix user:
If you want Phoenix to be deployed, you should repackage HBase tarball such that phoenix-server.jar is contained in the lib directory of the tarball.
e.g. for hbase-0.98.4.2.2.0.0-1799-hadoop2.tar.gz, phoenix-server.jar should be copied to:
 hbase-0.98.4.2.2.0.0-1799-hadoop2/lib

For Phoenix 4.4 and above, you need to specify "org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory" for "site.hbase-site.hbase.region.server.rpc.scheduler.factory.class". Also add "org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory" for "site.hbase-site.hbase.rpc.controllerfactory.class"

The version of HBase used for the app package can be adjusted by adding a
flag such as

    -Dhbase.version=0.98.3-hadoop2

Use the following command to install HBase tarball locally (under local workspace of HBase repo):

    mvn install:install-file -Dfile=<path-to-tarball> -DgroupId=org.apache.hbase -DartifactId=hbase -Dversion=0.98.3-hadoop2 -Dclassifier=bin -Dpackaging=tar.gz

You may need to copy the hbase tarball to the following location if the above step doesn't publish the tarball:

    ~/.m2/repository/org/apache/hbase/hbase/0.98.3-hadoop2/

After HBase tarball is published locally in maven repository, you can use the following command:

    mvn clean package -DskipTests -Phbase-app-package

App package can be found in

    app-packages/hbase/target/apache-slider-hbase-${hbase.version}-app-package-${slider.version}.zip

If an HBase version older than 0.98.3 is desired, it must be installed in the local maven repo.

A less descriptive file name can be specified with

    -Dapp.package.name=HBase_98dot3 which would create a file HBase_98dot3.zip.

## Verifying the content 

Verify the content using

    zip -Tv apache-slider-hbase-*.zip

# Extracting an HBase Client

The HBase app package provides scripts to assist in client interactions with
an HBase instance running on Slider.  These can be extracted to the HBase bin directory as follows.

    unzip slider-hbase-app-package*zip hbase-slider
    unzip slider-hbase-app-package*zip hbase-wrapper.py

The dir will then contain an HBase client that can be used
to connect to the cluster.

Example of command that invokes HBase shell is:

    SLIDER_HOME=</path/to/slider> ./hbase-slider <clusterName>
