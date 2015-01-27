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

# Creating a Slider app package for Accumulo

    mvn clean package -DskipTests -Paccumulo-app-package-maven

OR

    mvn clean package -DskipTests -Paccumulo-app-package -Dpkg.version=1.6.1 \
      -Dpkg.name=accumulo-1.6.1-bin.tar.gz -Dpkg.src=/local/path/to/tarball

The app package can be found in

    app-packages/accumulo/target/slider-accumulo-app-package-*.zip

In the first case, the version number of the app package will match the
slider version, and in the second case it will match the `pkg.version`
(intended to be the accumulo version).

Verify the content using

    zip -Tv slider-accumulo-app-package*.zip

`appConfig-default.json` and `resources-default.json` are not required to be
packaged.  These files are included as reference configuration for Slider apps
and are suitable for a one-node cluster.

In the maven packaging case, the version of Accumulo used for the app package
can be adjusted by adding a flag such as

    -Daccumulo.version=1.5.1

**Note** that the LICENSE.txt and NOTICE.txt that are bundled with the app
package are designed for Accumulo 1.6.0 only and may need to be modified to be
applicable for other versions of the app package.

Note also that the sample `appConfig-default.json` provided only works with
Accumulo 1.6 or greater.  For Accumulo 1.5 the instance.volumes property must be
replaced with `instance.dfs.dir` (and it cannot use the provided variable
`${DEFAULT_DATA_DIR}` which is an HDFS URI).

A less descriptive file name can be specified with
`-Dapp.package.name=accumulo_160` which would create a file `accumulo_160.zip`.

# Installing an Accumulo Client

The Accumulo app package provides scripts to assist in client interactions with
an Accumulo instance running on Slider.  These can be extracted as follows.

    unzip slider-accumulo-app-package*zip accumulo-slider
    unzip slider-accumulo-app-package*zip accumulo-slider.py

To install an Accumulo client, use the following command:

    SLIDER_HOME=</path/to/slider> ./accumulo-slider --app <clusterName> install <dir>

If the SLIDER_CONF_DIR is not at $SLIDER_HOME/conf, it should also be set.
The dir specified will then contain an Accumulo installation that can be used
to connect to the cluster.

Examples of other commands that may be issued are:

    SLIDER_HOME=</path/to/slider> ACCUMULO_HOME=</path/to/accumulo> ./accumulo-slider --app <clusterName> shell
    SLIDER_HOME=</path/to/slider> ACCUMULO_HOME=</path/to/accumulo> ./accumulo-slider --app <clusterName> --appconf <accumulo_conf_dir> getconf
    SLIDER_HOME=</path/to/slider> ACCUMULO_HOME=</path/to/accumulo> ./accumulo-slider --app <clusterName> quicklinks
    SLIDER_HOME=</path/to/slider> ACCUMULO_HOME=</path/to/accumulo> ./accumulo-slider --app <clusterName> proxies

# Building Native Libraries

Accumulo works better with its native libraries, and these must be built
manually for Accumulo releases 1.6.0 and greater.  They should be built on a
machine Accumulo will be deployed on, or an equivalent.  The procedure below
illustrates the steps for extracting and rebuilding the Accumulo app package
with native libraries, in the case of Accumulo version 1.6.0.  You will need a
C++ compiler/toolchain installed to build this library, and `JAVA_HOME` must be
set.

    unzip ${app.package.name}.zip package/files/accumulo*gz
    cd package/files/
    gunzip accumulo-1.6.0-bin.tar.gz
    tar xvf accumulo-1.6.0-bin.tar
    accumulo-1.6.0/bin/build_native_library.sh
    tar uvf accumulo-1.6.0-bin.tar accumulo-1.6.0
    rm -rf accumulo-1.6.0
    gzip accumulo-1.6.0-bin.tar
    cd ../../
    zip ${app.package.name}.zip -r package
    rm -rf package

# Export Control

This distribution includes cryptographic software. The country in which you
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any
encryption software, please check your country's laws, regulations and
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See
[http://www.wassenaar.org/](http://www.wassenaar.org/) for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security
(BIS), has classified this software as Export Commodity Control Number (ECCN)
5D002.C.1, which includes information security software using or performing
cryptographic functions with asymmetric algorithms. The form and manner of this
Apache Software Foundation distribution makes it eligible for export under the
License Exception ENC Technology Software Unrestricted (TSU) exception (see the
BIS Export Administration Regulations, Section 740.13) for both object code and
source code.

The following provides more details on the included cryptographic software:

Apache Slider uses the built-in java cryptography libraries. See Oracle's
information regarding Java cryptographic export regulations for more details:
[http://www.oracle.com/us/products/export/export-regulations-345813.html](http://www.oracle.com/us/products/export/export-regulations-345813.html)

Apache Slider uses the SSL libraries from the Jetty project distributed by the
Eclipse Foundation [http://eclipse.org/jetty](http://eclipse.org/jetty).

See also the Apache Accumulo export control notice in the README:
[http://accumulo.apache.org/downloads](http://accumulo.apache.org/downloads)
