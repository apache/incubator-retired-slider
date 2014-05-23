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
 
 These are just some templates for a pseudo-local cluster
 
 
 * Namenode [http://﻿http://localhost:50070/dfshealth.jsp](﻿http://localhost:50070/dfshealth.jsp)
 * YARN RM [﻿http://localhost:9081/cluster](﻿http://localhost:9081/cluster)
 
 # Core settings
 
     <configuration>
       <property>
         <name>fs.defaultFS</name>
         <value>hdfs://localhost:9000</value>
       </property>
     </configuration>
     <property>
       <name>yarn.resourcemanager.address</name>
       <value>localhost:8032</value>
     </property>
 
 
 For the slider command line
 
    --manager localhost:9080 --filesystem hdfs://localhost:9000 --zkhosts localhost
 
 
