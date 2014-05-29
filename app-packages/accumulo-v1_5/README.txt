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

How to create a Slider package?

Replace the placeholder tarball for Accumulo.
  cp ~/Downloads/accumulo-1.5.1-bin.tar.gz package/files/
  rm package/files/accumulo-1.5.1-bin.tar.gz.REPLACE

Create a zip package at the root of the package (<slider enlistment>/app-packages/accumulo-v1_5/) 
  zip -r accumulo_v151.zip .

Verify the content using  
  unzip -l "$@" accumulo_v151.zip

While appConfig.json and resources.json are not required for the package they work
well as the default configuration for Slider apps. So its advisable that when you
create an application package for Slider, include sample/default resources.json and
appConfig.json for a minimal Yarn cluster.
