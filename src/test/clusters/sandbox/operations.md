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

# Just some operations for manual runs against steve's secure VM


    export SLIDER_CONF_DIR=/home/slider/projects/slider/slider-core/src/test/configs/sandbox/slider

## Local manual tests



    slider create cluster1 \
         --component worker 4\
          --zkhosts sandbox:2181 \
          --provider hbase \
          --image hdfs://sandbox:8020/user/slider/hbase.tar.gz \
          --appconf file:////home/slider/Hadoop/configs/master/hbase \
          --compopt master jvm.heap 128 \
          --compopt worker jvm.heap 128 

 
### bypassing /etc/krb.conf via the -S argument

    bin/slider create cl1 \
    --manager sandbox:8032 --filesystem hdfs://sandbox:8020 \
            --component worker 1\
            --component master 0\
        --zkhosts sandbox:2181  \
        --provider hbase \
        --image hdfs://sandbox:8020/user/slider/hbase.tar.gz \
        --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
        --compopt master jvm.heap 128 \
        --compopt master env.MALLOC_ARENA_MAX 4 \
        --compopt worker jvm.heap 128 
        


    bin/slider create cl1 \
        --component master 0 \
        --zkhosts sandbox:2181  \
        --image hdfs://sandbox:8020/user/slider/hbase.tar.gz \
        --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
        --compopt master jvm.heap 128 \
        --compopt master env.MALLOC_ARENA_MAX 4 
        
                
        
    bin/slider status clu1 \
    --manager sandbox:8032 --filesystem hdfs://sandbox:8020 \
           
    bin/slider list \
    --manager sandbox:8032 \
               

               
# single master & workre
     
    bin/slider create cluster3 \
    --zkhosts sandbox:2181  \
    --provider hbase \
    --image hdfs://sandbox:8020/user/slider/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
    --component master 1 \
    --component worker 1 
    
    
# one master
     
    bin/slider create cl1 \
    --zkhosts sandbox:2181   \
    --provider hbase \
    --image hdfs://sandbox:8020/user/slider/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
    --component master 1 

# one master env set up
      
     bin/slider create cl1 \
     --zkhosts sandbox:2181   \
     --provider hbase \
     --image hdfs://sandbox:8020/user/slider/hbase.tar.gz \
     --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
     --component master 1  \
     --component worker 1  
    
# build but don't deploy single master
     
    bin/slider build cl1 \
    --zkhosts sandbox:2181 \
    --provider hbase \
    --image hdfs://sandbox:8020/user/slider/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
    --component master 1 
         

               
    bin/slider  status cl1 
    
    
    bin/slider  status cl1 
     
   
     
     
               
    bin/slider  start cl1
                   
    bin/slider  stop cl1
    bin/slider  stop cluster3
    bin/slider  destroy cl1  
    
    
      
         
    bin/slider  emergency-force-kill all 
     
     
## All configured 
     
     
    bin/slider create cl1 \
      --component worker 1\
      --component master 2\
      --zkhosts sandbox:2181 \
      --provider hbase \
      --image hdfs://sandbox:8020/user/slider/hbase.tar.gz  \
      --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
      --compopt master env.MALLOC_ARENA_MAX 4 \
      --compopt worker app.infoport 0 \
  
### flex the cluster
  
     bin/slider flex cl1 \
      --component master 1 \
      --component worker 2 
    
### stop

    bin/slider  stop cl1
    
    bin/slider  stop cl1 --force
    
### start

    bin/slider  start cl1 -D slider.yarn.queue.priority=5 -D slider.yarn.queue=default
    
    
### start with bad queue: _MUST_ fail
    
    bin/slider  start cl1 -D slider.yarn.queue=unknown
     
### monitor

    bin/slider  monitor cl1      

### list all

    bin/slider  list
     
### list

    bin/slider  list cl1 
    
### status

    bin/slider  status cl1 
    
### destroy

    bin/slider  destroy cl1 
    
    
