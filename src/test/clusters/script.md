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
  
 # script
 
 
 # starting
 
 

## env

    export HADOOP_CONF_DIR=/home/stevel/conf
    export PATH=/home/stevel/hadoop/bin:/home/stevel/hadoop/sbin:$PATH


## filesystem

    hdfs namenode -format vm



## start all the services

    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode
    yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
    yarn-daemon.sh --config $HADOOP_CONF_DIR start nodemanager
    zookeeper-3.4.5/bin/zkServer.sh start

  

* NN up on (http://localhost:50070/dfshealth.jsp)[http://localhost:50070/dfshealth.jsp]



## shutdown

./zookeeper-3.4.5/bin/zkServer.sh stop


## Windows
 
