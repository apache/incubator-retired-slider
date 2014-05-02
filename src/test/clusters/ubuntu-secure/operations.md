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

  export SLIDER_JVM_OPTS="-Djava.security.krb5.realm=COTHAM -Djava.security.krb5.kdc=ubuntu -Djava.net.preferIPv4Stack=true"


## Local manual tests



    
    slider-assembly/target/slider-assembly-0.5.1-SNAPSHOT-bin/bin/slider \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 list -D slider.security.enabled=true
      
      slider create cluster1 \
          --provider hbase \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
         --role workers 4\
          --zkhosts ubuntu --zkport 2121 \
          -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM \
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
    -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
            --role worker 1\
            --role master 0\
        --zkhosts ubuntu --zkport 2121 \
        --image hdfs://ubuntu:9090/hbase.tar \
        --appconf file:///Users/stevel/Projects/Hortonworks/Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 \
        --roleopt worker jvm.heap 128 
        


    bin/slider create cl1 \
          --provider hbase \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
        --role master 0 \
        --zkhosts ubuntu --zkport 2121 \
        --image hdfs://ubuntu:9090/hbase.tar \
        --appconf file:///Users/stevel/Projects/Hortonworks/Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
        --roleopt master jvm.heap 128 \
        --roleopt master env.MALLOC_ARENA_MAX 4 
        
                
        
    bin/slider status clu1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM \
    -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
           
    bin/slider list \
    --manager ubuntu:8032 \
    -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
      -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM
               
               

               
# single master & workre
     
    bin/slider create cluster3 \
          --provider hbase \
    --zkhosts ubuntu --zkport 2121 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
    -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
    -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    --image hdfs://ubuntu:9090/hbase.tar \
    --appconf file:///Users/stevel/Projects/Hortonworks/Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
    --roleopt master app.infoport 8080  \
    --role master 1 \
    --role worker 1 
    
    
# one master
     
    bin/slider create cl1 \
      --provider hbase \
      --zkhosts ubuntu  --zkport 2121 \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
      -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
      -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
      -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
      --image hdfs://ubuntu:9090/hbase.tar \
      --appconf file:///Users/stevel/Projects/Hortonworks/Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
      --role master 1 

# one master env set up
      
     bin/slider create cl1 \
          --provider hbase \
         --zkhosts ubuntu  --zkport 2121 \
         --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
         -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
         -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
         -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
         --image hdfs://ubuntu:9090/hbase.tar \
         --appconf file:///Users/stevel/Projects/Hortonworks/Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
         --role master 1  \
         --role worker 1  
    
# build but don't deploy single master
     
    bin/slider build cl1 \
          --provider hbase \
      --zkhosts ubuntu \
      --zkport 2121 \
      --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
      -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
      -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
      -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
      --image hdfs://ubuntu:9090/hbase.tar \
      --appconf file:///Users/stevel/Projects/Hortonworks/Projects/slider/slider-funtest/src/test/configs/ubuntu-secure/hbase \
      --role master 1 
         
               
    bin/slider  status cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
     -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
               
    bin/slider  status cl1 -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu 
    
    
    bin/slider  status cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
   bin/slider  status cluster3 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
     -D slider.security.enabled=true \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
     
     
               
    bin/slider  thaw cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true \
     -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
                   
    bin/slider  freeze cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM   
                      
    bin/slider  freeze cluster3 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM 
    
    bin/slider  destroy cl1 \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true \
    -S java.security.krb5.realm=COTHAM -S java.security.krb5.kdc=ubuntu \
     -D yarn.resourcemanager.principal=yarn/ubuntu@COTHAM \
     -D dfs.namenode.kerberos.principal=hdfs/ubuntu@COTHAM \
    
    
      
         
    bin/slider  emergency-force-kill all \
    --manager ubuntu:8032 --filesystem hdfs://ubuntu:9090 \
    -D slider.security.enabled=true -S java.security.krb5.realm=COTHAM \
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
      --appconf file:///Users/stevel/Projects/Hortonworks/Projects/hoya/hoya-funtest/src/test/configs/ubuntu-secure/hbase \
      --roleopt master env.MALLOC_ARENA_MAX 4 \
      --roleopt worker app.infoport 0 \
  
# flex the cluster
  
     bin/slider flex cl1 \
      --role master 1 \
      --role worker 2 
    
# freeze

    bin/slider  freeze cl1 
    
# thaw

    bin/slider  thaw cl1
     
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
    
