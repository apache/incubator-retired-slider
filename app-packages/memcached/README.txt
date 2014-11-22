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

How to create a Slider app package for Memcached?

To create the app package you will need the Memcached tarball copied to a specific location.

Replace the placeholder tarball for JMemcached. The tarball must have all the jar files at the
root directory.
Example:
  tar -tvf jmemcached-1.0.0.tar
  -rw-r--r--  ./jmemcached-cli-1.0.0.jar
  -rwxr-xr-x  ./jmemcached-core-1.0.0.jar

If not modify, appConfig.json to have correct application install root.
  "site.global.app_root": "${AGENT_WORK_ROOT}/app/install/my_sub_root_for_jars",

  cp ~/Downloads/jmemcached-1.0.0.tar package/files/
  rm package/files/jmemcached-1.0.0.tar.REPLACE

Create a zip package at the root of the package (<slider enlistment>/app-packages/memcached/)
  zip -r jmemcached-1.0.0.zip .

Verify the content using  
  unzip -l "$@" jmemcached-1.0.0.zip

appConfig-default.json and resources-default.json are not required to be packaged.
These files are included as reference configuration for Slider apps and are suitable
for a one-node cluster.
