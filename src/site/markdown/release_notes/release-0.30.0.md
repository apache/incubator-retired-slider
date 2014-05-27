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
  
# Slider Release 0.30.0

May 2014

This release is built against the Apache Hadoop 2.4.0, HBase-0.98.1
and Accumulo 1.5.1 artifacts. 

Download: []()


## Key changes
1. Slider application registry that allow registration and discovery of application configuration and URLs (such as jmx endpoints and management UI) for client consumption.
2. Move to a .zip packaging for Slider application packages.
3. Richer metainfo support to provide start ordering and arbitrary template that can be published. 

## Other changes

1. [SLIDER-13](https://issues.apache.org/jira/browse/SLIDER-13) switch build to be java7+ only.