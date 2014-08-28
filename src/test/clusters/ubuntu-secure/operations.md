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

# Just some operations for manual runs against a kerberized Ubunt VM

# starting services

## using hadoop/sbin service launchers
    
    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode
    yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
    yarn-daemon.sh --config $HADOOP_CONF_DIR start nodemanager
    
    ~/zookeeper/bin/zkServer.sh start
    
    
## stop them

    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs stop namenode
    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs stop datanode
    
    yarn-daemon.sh --config $HADOOP_CONF_DIR stop resourcemanager
    yarn-daemon.sh --config $HADOOP_CONF_DIR stop nodemanager
    
    ~/zookeeper/bin/zkServer.sh stop


  export SLIDER_JVM_OPTS="-Djava.security.krb5.realm=COTHAM -Djava.security.krb5.kdc=ubuntu -Djava.net.preferIPv4Stack=true"


## Local manual tests



    
    slider-assembly/target/slider-assembly-0.5.1-SNAPSHOT-bin/bin/slider \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 list -D slider.security.enabled=true
      
      slider create cluster1 \
          --provider hbase \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
         --role workers 4\
          --zkhosts ubuntu --zkport 2121 \
          -S java.security.krb5.realm=COTHAM \
          -S java.security.krb5.kdc=ubuntu \
          --image hdfs://ubuntu:9090/hbase.tar \
          --appconf file:////Users/slider/Hadoop/configs/master/hbase \
          --roleopt master jvm.heap 128 \
          --roleopt master env.MALLOC_ARENA_MAX 4 \
          --roleopt worker jvm.heap 128 

 
### bypassing /etc/krb.conf via the -S argument

    bin/slider create cl1 \
          --provider hbase \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
            --role worker 1\
            --role master 0\
        --zkhosts ubuntu --zkport 2121 \
        --image hdfs://ubuntu:9090/hbase.tar \
        --appconf file:///home/slider/projects//Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 \
        --roleopt worker jvm.heap 128 
        


    bin/slider create cl1 \
          --provider hbase \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
        --role master 0 \
        --zkhosts ubuntu --zkport 2121 \
        --image hdfs://ubuntu:9090/hbase.tar \
        --appconf file:///home/slider/projects//Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 
        
                
        
    bin/slider status clu1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
           
    bin/slider list \
    --manager ubuntu:8032 \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
      -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM
               
               

               
# single master & workre
     
    bin/slider create cluster3 \
          --provider hbase \
    --zkhosts ubuntu --zkport 2121 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///home/slider/projects//Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
    --roleopt master app.infoport 8080  \
    --role master 1 \
    --role worker 1 
    
    
# one master
     
    bin/slider create cl1 \
      --provider hbase \
      --zkhosts ubuntu  --zkport 2121 \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
      -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
      -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
      -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
      --image hdfs://ubuntu:9090/hbase.tar \
      --appconf file:///home/slider/projects//Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
      --role master 1 

# one master env set up
      
     bin/slider create cl1 \
          --provider hbase \
         --zkhosts ubuntu  --zkport 2121 \
         --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
         -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
         -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
         -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
         --image hdfs://ubuntu:9090/hbase.tar \
         --appconf file:///home/slider/projects//Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
         --role master 1  \
         --role worker 1  
    
# build but don't deploy single master
     
    bin/slider build cl1 \
          --provider hbase \
      --zkhosts ubuntu \
      --zkport 2121 \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
      -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
      -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
      -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
      --image hdfs://ubuntu:9090/hbase.tar \
      --appconf file:///home/slider/projects//Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
      --role master 1 
         
               
    bin/slider  status cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
     -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
               
    bin/slider  status cl1 -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu 
    
    
    bin/slider  status cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
   bin/slider  status cluster3 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
     \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
     
               
    bin/slider  start cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    \
     -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
                   
    bin/slider  stop cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM   
                      
    bin/slider  stop cluster3 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
    
    bin/slider  destroy cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    
    
      
         
    bin/slider  emergency-force-kill all \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -S java.security.krb5.realm=COTHAM \
     -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
     
## All configured 
     
     
    bin/slider create cl1 \
      -S java.security.krb5.realm=COTHAM \
      -S java.security.krb5.kdc=ubuntu \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
      --role worker 1\
      --role master 2\
      --zkhosts ubuntu \
      --zkport 2121 \
      --image hdfs://ubuntu:9090/hbase.tar \
      --appconf file:///home/slider/projects//Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
      --roleopt master env.MALLOC_ARENA_MAX 4 \
      --roleopt worker app.infoport 0 \
  
# flex the cluster
  
     bin/slider flex cl1 \
      --role master 1 \
      --role worker 2 
    
# stop

    bin/slider  stop cl1
    
# start

    bin/slider  start cl1
     
# monitor

    bin/slider  monitor cl1      

# list all

    bin/slider  list
     
# list

    bin/slider  list cl1 
    
# status

    bin/slider  status cl1 
    
# destroy

    bin/slider  destroy cl1 
    
