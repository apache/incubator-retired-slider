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
  
 # README
 
This is a set of configurations for a single-node YARN cluster running in
a VM, with HDFS and YARN brought up on the external network, not localhost.
Assuming the physical host has a DNS entry for the machine, *ubuntu*, 
a Slider Client running on the host can upload Slider and deploy HBase onto
the single-node cluster even though neither is installed on the VM.
 
 
 * Namenode [http:///ubuntu:50070/dfshealth.jsp](﻿http://ubuntu:50070/dfshealth.jsp)
 * YARN RM [﻿http://ubuntu:9081/cluster](﻿http://ubuntu:9081/cluster)
 
 # Core settings
 
     <configuration>
       <property>
         <name>fs.defaultFS</name>
         <value>hdfs://ubuntu:9090</value>
       </property>
     </configuration>
     <property>
       <name>yarn.resourcemanager.address</name>
       <value>ubuntu:8032</value>
     </property>
 
 
 For the slider command line
 
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 --zkhosts localhost
 
 
