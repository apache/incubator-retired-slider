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
 
This is a set of configurations for a single-node YARN cluster built
against a sandbox VM, *with a DNS entry "sandbox" to match*


 
 * Namenode [http:///ubuntu:50070/dfshealth.jsp](﻿http://ubuntu:50070/dfshealth.jsp)
 * YARN RM [﻿http://ubuntu:9081/cluster](﻿http://ubuntu:9081/cluster)
 
 # Core settings
 
     <configuration>
       <property>
         <name>fs.defaultFS</name>
         <value>hdfs://sandbox:9090</value>
       </property>

       <property>
         <name>yarn.resourcemanager.address</name>
         <value>sandbox:8032</value>
       </property>
       
       <property>
         <name>slider.zookeeper.quorum</name>
         <value>sandbox:2181</value>
       </property>

     </configuration>
 
 For the slider command line
 
    --manager sandbox:8032 --filesystem hdfs://sandbox:9090 
 
 
