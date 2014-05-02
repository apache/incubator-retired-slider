<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Just some operations for manual runs against steve's secure VM


    export HOYA_CONF_DIR=/Users/stevel/Projects/Hortonworks/Projects/slider/slider-core/src/test/configs/sandbox/hoya

## Local manual tests



    slider create cluster1 \
         --component worker 4\
          --zkhosts sandbox:2181 \
          --provider hbase \
          --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
          --appconf file:////Users/hoya/Hadoop/configs/master/hbase \
          --compopt master jvm.heap 128 \
          --compopt worker jvm.heap 128 

 
### bypassing /etc/krb.conf via the -S argument

    bin/slider create cl1 \
    --manager sandbox:8032 --filesystem hdfs://sandbox.hortonworks.com:8020 \
            --component worker 1\
            --component master 0\
        --zkhosts sandbox:2181  \
        --provider hbase \
        --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
        --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
        --compopt master jvm.heap 128 \
        --compopt master env.MALLOC_ARENA_MAX 4 \
        --compopt worker jvm.heap 128 
        


    bin/slider create cl1 \
        --component master 0 \
        --zkhosts sandbox:2181  \
        --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
        --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
        --compopt master jvm.heap 128 \
        --compopt master env.MALLOC_ARENA_MAX 4 
        
                
        
    bin/slider status clu1 \
    --manager sandbox:8032 --filesystem hdfs://sandbox.hortonworks.com:8020 \
           
    bin/slider list \
    --manager sandbox:8032 \
               

               
# single master & workre
     
    bin/slider create cluster3 \
    --zkhosts sandbox:2181  \
    --provider hbase \
    --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
    --component master 1 \
    --component worker 1 
    
    
# one master
     
    bin/slider create cl1 \
    --zkhosts sandbox:2181   \
    --provider hbase \
    --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
    --component master 1 

# one master env set up
      
     bin/slider create cl1 \
     --zkhosts sandbox:2181   \
     --provider hbase \
     --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
     --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
     --component master 1  \
     --component worker 1  
    
# build but don't deploy single master
     
    bin/slider build cl1 \
    --zkhosts sandbox:2181 \
    --provider hbase \
    --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz \
    --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
    --component master 1 
         

               
    bin/slider  status cl1 
    
    
    bin/slider  status cl1 
     
   
     
     
               
    bin/slider  thaw cl1  
                   
    bin/slider  freeze cl1  
    bin/slider  freeze cluster3  
    bin/slider  destroy cl1  
    
    
      
         
    bin/slider  emergency-force-kill all 
     
     
## All configured 
     
     
    bin/slider create cl1 \
      --component worker 1\
      --component master 2\
      --zkhosts sandbox:2181 \
      --provider hbase \
      --image hdfs://sandbox.hortonworks.com:8020/user/hoya/hbase.tar.gz  \
      --appconf file:///Users/stevel/Projects/slider/slider-core/src/test/configs/sandbox/hbase \
      --compopt master env.MALLOC_ARENA_MAX 4 \
      --compopt worker app.infoport 0 \
  
### flex the cluster
  
     bin/slider flex cl1 \
      --component master 1 \
      --component worker 2 
    
### freeze

    bin/slider  freeze cl1 
    
    bin/slider  freeze cl1 --force 
    
### thaw

    bin/slider  thaw cl1 -D slider.yarn.queue.priority=5 -D slider.yarn.queue=default
    
    
### thaw with bad queue: _MUST_ fail
    
    bin/slider  thaw cl1 -D slider.yarn.queue=unknown
     
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
    
    