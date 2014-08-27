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

Download the tarball for HBase:
  e.g. path to tarball ~/Downloads/hbase-0.98.0.2.1.5.0-2047-hadoop2.zip

Copy the hbase tarball to package/files
  cp ~/Downloads/hbase-0.98.0.2.1.5.0-2047-hadoop2.zip package/files
  rm -rf package/files/hbase-0.98.0.2.1.5.0-2047-hadoop2.zip.REPLACEME

Edit appConfig.json/metainfo.xml and replace all occurances of 
0.98.0.2.1.5.0-2047-hadoop2 with the correct value.

Create a zip package at the root of the package (<slider enlistment>/app-packages/hbase-win/)
  zip -r hbase-win-v098.zip .

Verify the content using
  zip -Tv hbase-win-v098.zip
