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

# Examples

 
## Setup
 
### Setting up a YARN cluster
 
For simple local demos, a Hadoop pseudo-distributed cluster will suffice -if on a VM then
its configuration should be changed to use a public (machine public) IP.

# The examples below all assume there is a cluster node called 'master', which
hosts the HDFS NameNode and the YARN Resource Manager


# preamble

    export HADOOP_CONF_DIR=~/conf
    export PATH=~/hadoop/bin:/~/hadoop/sbin:~/zookeeper-3.4.5/bin:$PATH
    
    hdfs namenode -format master
  



# start all the services

    nohup hdfs --config $HADOOP_CONF_DIR namenode & 
    nohup hdfs --config $HADOOP_CONF_DIR datanode &
    
    
    nohup yarn --config $HADOOP_CONF_DIR resourcemanager &
    nohup yarn --config $HADOOP_CONF_DIR nodemanager &
    
# using hadoop/sbin service launchers
    
    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode
    yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
    yarn-daemon.sh --config $HADOOP_CONF_DIR start nodemanager
    
    ~/zookeeper/bin/zkServer.sh start
    
    
# stop them

    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs stop namenode
    hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs stop datanode
    
    yarn-daemon.sh --config $HADOOP_CONF_DIR stop resourcemanager
    yarn-daemon.sh --config $HADOOP_CONF_DIR stop nodemanager
    


NN up on [http://master:50070/dfshealth.jsp](http://master:50070/dfshealth.jsp)
RM yarn-daemon.sh --config $HADOOP_CONF_DIR start nodemanager

    ~/zookeeper/bin/zkServer.sh start


    # shutdown
    ~/zookeeper/bin/zkServer.sh stop


Tip: after a successful run on a local cluster, do a quick `rm -rf $HADOOP_HOME/logs`
to keep the log bloat under control.

## get hbase in

copy to local 

    get hbase-0.98.0-bin.tar on 


    hdfs dfs -rm hdfs://master:9090/hbase.tar
    hdfs dfs -copyFromLocal hbase-0.98.0-bin.tar hdfs://master:9090/hbase.tar

or
    
    hdfs dfs -copyFromLocal hbase-0.96.0-bin.tar hdfs://master:9090/hbase.tar
    hdfs dfs -ls hdfs://master:9090/
    

### Optional: point bin/slider at your chosen cluster configuration

export SLIDER_CONF_DIR=~/Projects/slider/slider-core/src/test/configs/ubuntu-secure/slider

## Optional: Clean up any existing slider cluster details

This is for demos only, otherwise you lose the clusters and their databases.

    hdfs dfs -rm -r hdfs://master:9090/user/home/stevel/.hoya

## Create a Slider Cluster
 
 
    slider  create cl1 \
    --component worker 1  --component master 1 \
     --manager master:8032 --filesystem hdfs://master:9090 \
     --zkhosts localhost:2181 --image hdfs://master:9090/hbase.tar
    
    # create the cluster
    
    slider create cl1 \
     --component worker 4 --component master 1 \
      --manager master:8032 --filesystem hdfs://master:9090 --zkhosts localhost \
      --image hdfs://master:9090/hbase.tar \
      --appconf file:////Users/slider/Hadoop/configs/master/hbase \
      --compopt master jvm.heap 128 \
      --compopt master env.MALLOC_ARENA_MAX 4 \
      --compopt worker jvm.heap 128 

    # freeze the cluster
    slider freeze cl1 \
    --manager master:8032 --filesystem hdfs://master:9090

    # thaw a cluster
    slider thaw cl1 \
    --manager master:8032 --filesystem hdfs://master:9090

    # destroy the cluster
    slider destroy cl1 \
    --manager master:8032 --filesystem hdfs://master:9090

    # list clusters
    slider list cl1 \
    --manager master:8032 --filesystem hdfs://master:9090
    
    slider flex cl1 --component worker 2
    --manager master:8032 --filesystem hdfs://master:9090 \
    --component worker 5
    
## Create an Accumulo Cluster

    slider create accl1 --provider accumulo \
    --component master 1 --component tserver 1 --component gc 1 --component monitor 1 --component tracer 1 \
    --manager localhost:8032 --filesystem hdfs://localhost:9000 \
    --zkhosts localhost:2181 --zkpath /local/zookeeper \
    --image hdfs://localhost:9000/user/username/accumulo-1.6.0-SNAPSHOT-bin.tar \
    --appconf hdfs://localhost:9000/user/username/accumulo-conf \
    -O zk.home /local/zookeeper -O hadoop.home /local/hadoop \
    -O site.monitor.port.client 50095 -O accumulo.password secret 
    
