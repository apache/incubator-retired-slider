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

# Testing

     The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL
      NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and
      "OPTIONAL" in this document are to be interpreted as described in
      RFC 2119.

## Standalone Tests

Slider core contains a suite of tests that are designed to run on the local machine,
using Hadoop's `MiniDFSCluster` and `MiniYARNCluster` classes to create small,
one-node test clusters. All the YARN/HDFS code runs in the JUnit process; the
AM and spawned processeses run independently.



### For HBase Tests in `slider-providers/hbase`

Requirements
* A copy of `hbase.tar.gz` in the local filesystem
* A an expanded `hbase.tar.gz` in the local filesystem


### For Accumulo Tests in `slider-providers/accumulo`
* A copy of `accumulo.tar.gz` in the local filesystem, 
* An expanded `accumulo.tar.gz` in the local filesystem, 
* an expanded Zookeeper installation

All of these need to be defined in the file `slider-core/src/test/resources/slider-test.xml`

Example:
  
    <configuration>
    
      <property>
        <name>slider.test.hbase.enabled</name>
        <description>Flag to enable/disable HBase tests</description>
        <value>true</value>
      </property>
      
      <property>
        <name>slider.test.hbase.home</name>
        <value>/home/slider/hbase-0.98.0</value>
        <description>HBASE Home</description>
      </property>
    
      <property>
        <name>slider.test.hbase.tar</name>
        <value>/home/slider/Projects/hbase-0.98.0-bin.tar.gz</value>
        <description>HBASE archive URI</description>
      </property>
    
      <property>
        <name>slider.test.accumulo.enabled</name>
        <description>Flag to enable/disable Accumulo tests</description>
        <value>true</value>
      </property>
    
      <property>
        <name>slider.test.accumulo.home</name>
        <value>
          /home/slider/accumulo-1.6.0-SNAPSHOT/</value>
        <description>Accumulo Home</description>
      </property>
    
      <property>
        <name>slider.test.accumulo.tar</name>
        <value>/home/slider/accumulo-1.6.0-SNAPSHOT-bin.tar</value>
        <description>Accumulo archive URI</description>
      </property>

      <property>
        <name>slider.test.am.restart.time</name>
        <description>Time in millis to await an AM restart</description>
        <value>30000</value>
      </property>

      <property>
        <name>zk.home</name>
        <value>/home/slider/zookeeper</value>
        <description>Zookeeper home dir on target systems</description>
      </property>
    
      <property>
        <name>hadoop.home</name>
        <value>/home/slider/hadoop-2.2.0</value>
        <description>Hadoop home dir on target systems</description>
      </property>
      
    </configuration>

*Important:* For the local tests, a simple local filesystem path is used for
all the values. 

For the functional tests, the accumulo and hbase tar properties will
need to be set to a URL of a tar file that is accessible to all the
nodes in the cluster -which usually means HDFS, and so an `hdfs://` URL


##  Provider-specific parameters

An individual provider can pick up settings from their own
`src/test/resources/slider-client.xml` file, or the one in `slider-core`.
We strongly advice placing all the values in the `slider-core` file.

1. All uncertainty about which file is picked up on the class path first goes
away
2. There's one place to  keep all the configuration values in sync.

### Agent Tests


### HBase Tests

The HBase tests can be enabled or disabled
    
    <property>
      <name>slider.test.hbase.enabled</name>
      <description>Flag to enable/disable HBase tests</description>
      <value>true</value>
    </property>
        
Mandatory test parameters must be added to `slider-client.xml`

  
    <property>
      <name>slider.test.hbase.tar</name>
      <description>Path to the HBase Tar file in HDFS</description>
      <value>hdfs://sandbox:8020/user/slider/hbase.tar.gz</value>
    </property>
    
    <property>
      <name>slider.test.hbase.appconf</name>
      <description>Path to the directory containing the HBase application config</description>
      <value>file://${user.dir}/src/test/configs/sandbox/hbase</value>
    </property>
    
Optional parameters:  
  
     <property>
      <name>slider.test.hbase.launch.wait.seconds</name>
      <description>Time to wait in seconds for HBase to start</description>
      <value>180000</value>
    </property>  


#### Accumulo configuration options

Enable/disable the tests

     <property>
      <name>slider.test.accumulo.enabled</name>
      <description>Flag to enable/disable Accumulo tests</description>
      <value>true</value>
     </property>
         
         
Optional parameters
         
     <property>
      <name>slider.test.accumulo.launch.wait.seconds</name>
      <description>Time to wait in seconds for Accumulo to start</description>
      <value>180000</value>
     </property>

