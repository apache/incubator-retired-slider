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

# Functional Tests

The functional test suite is designed to run the executables against
a live cluster. 

For these to work you need
1. A YARN Cluster -secure or insecure
1. A `slider-client.xml` file configured to interact with the cluster
1. Agent 
1. HBase tests:  HBase `.tar.gz` uploaded to HDFS, and a local or remote accumulo conf 
directory
1. Accumulo Tests Accumulo `.tar.gz` uploaded to HDFS, and a local or remote accumulo conf 
directory

## Configuration of functional tests

Maven needs to be given 
1. A path to the expanded test archive
1. A path to a slider configuration directory for the cluster

The path for the expanded test is automatically calculated as being the directory under
`..\slider-assembly\target` where an untarred slider distribution can be found.
If it is not present, the tests will fail

The path to the configuration directory must be supplied in the property
`slider.conf.dir` which can be set on the command line

    mvn test -Dslider.conf.dir=src/test/configs/sandbox/slider

It can also be set in the (optional) file `slider-funtest/build.properties`:

    slider.conf.dir=src/test/configs/sandbox/slider

This file is loaded whenever a slider build or test run takes place

## Configuration of `slider-client.xml`

The `slider-client.xml` must have extra configuration options for both the HBase and
Accumulo tests, as well as a common set for actually talking to a YARN cluster.

## Disabling the functional tests entirely

All functional tests which require a live YARN cluster
can be disabled through the property `slider.funtest.enabled`
  
    <property>
      <name>slider.funtest.enabled</name>
      <value>false</value>
    </property>

There is a configuration do do exactly this in
`src/test/configs/offline/slider`:

    slider.conf.dir=src/test/configs/offline/slider

Tests which do not require a live YARN cluster will still run;
these verify that the `bin/slider` script works.

### Non-mandatory options

The following test options may be added to `slider-client.xml` if the defaults
need to be changed
                   
    <property>
      <name>slider.test.zkhosts</name>
      <description>comma separated list of ZK hosts</description>
      <value>localhost</value>
    </property>
       
    <property>
      <name>slider.test.thaw.wait.seconds</name>
      <description>Time to wait in seconds for a thaw to result in a running AM</description>
      <value>60000</value>
    </property>
    
    <property>
      <name>slider.test.freeze.wait.seconds</name>
      <description>Time to wait in seconds for a freeze to halt the cluster</description>
      <value>60000</value>
    </property>
            
     <property>
      <name>slider.test.timeout.millisec</name>
      <description>Time out in milliseconds before a test is considered to have failed.
      There are some maven properties which also define limits and may need adjusting</description>
      <value>180000</value>
    </property>
    
    
    
Note that while the same properties need to be set in
`slider-core/src/test/resources/slider-client.xml`, those tests take a file in the local
filesystem -here a URI to a path visible across all nodes in the cluster are required
the tests do not copy the .tar/.tar.gz files over. The application configuration
directories may be local or remote -they are copied into the `.slider` directory
during cluster creation.

##  Provider-specific parameters

An individual provider can pick up settings from their own
`src/test/resources/slider-client.xml` file, or the one in `slider-core`.
We strongly advice placing all the values in the `slider-core` file.

1. All uncertainty about which file is picked up on the class path first goes
away
2. There's one place to  keep all the configuration values in sync.

### Agent Tests

    <property>
      <name>slider.test.agent.enabled</name>
      <description>Flag to enable/disable Agent tests</description>
      <value>true</value>
    </property>
        
The agent tarball needs to be pushed to HBase. This is the tar
file created
        
    <property>
      <name>slider.test.agent.tar</name>
      <description>Path to the Agent Tar file in HDFS</description>
      <value>hdfs://sandbox:8020/user/slider/agent.tar.gz</value>
    </property>


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



### Configuring the YARN cluster for tests


Here are the configuration options we use in `yarn-site.xml` for testing:

These tell YARN to ignore memory requirements in allocating VMs, and
to keep the log files around after an application run. 

      <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>1</value>
      </property>
      <property>
        <description>Whether physical memory limits will be enforced for
          containers.
        </description>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
      </property>
      <!-- we really don't want checking here-->
      <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
      </property>
      
      <!-- how long after a failure to see what is left in the directory-->
      <property>
        <name>yarn.nodemanager.delete.debug-delay-sec</name>
        <value>60000</value>
      </property>
    
      <!--ten seconds before the process gets a -9 -->
      <property>
        <name>yarn.nodemanager.sleep-delay-before-sigkill.ms</name>
        <value>30000</value>
      </property>


### Testing against a secure cluster

To test against a secure cluster

1. `slider-client.xml` must be configured as per [Security](../security.html).
1. the client must have the kerberos tokens issued so that the user running
the tests has access to HDFS and YARN.

If there are problems authenticating (including the cluster being offline)
the tests appear to hang

### Validating the configuration

    mvn test -Dtest=TestBuildSetup

### Using relative paths in test configurations

When you are sharing configurations across machines via SCM or similar,
its impossible to have absolute paths in the configuration options to
the location of items in the local filesystem (e.g. configuration directories).

There's two techniques

1. Keep the data in HDFS and refer to it there. This works if there is a shared,
persistent HDFS cluster.

1. Use the special property `slider.test.conf.dir` that is set to the path
of the directory, and which can then be used to create an absolute path
from paths relative to the configuration dir:

	    <property>
    	  <name>slider.test.hbase.appconf</name>
    	  <description>Path to the directory containing the HBase application config</description>
    	  <value>file://${slider.test.conf.dir}/../hbase</value>
    	</property>


If the actual XML file path is required, a similar property
`slider.test.conf.xml` is set.


## Parallel execution

Attempts to run test cases in parallel failed -even with a configuration
to run methods in a class sequentially, but separate classes independently.

Even after identifying and eliminating some unintended sharing of static
mutable variables, trying to run test cases in parallel seemed to hang
tests and produce timeouts.

For this reason parallel tests have been disabled. To accelerate test runs
through parallelization, run different tests on different hosts instead.

## Other constraints

* Port assignments SHOULD NOT be fixed, as this will cause clusters to fail if
there are too many instances of a role on a same host, or if other tests are
using the same port.
* If a test does need to fix a port, it MUST be for a single instance of a role,
and it must be different from all others. The assignment should be set in 
`org.apache.slider.funtest.itest.PortAssignments` so as to ensure uniqueness
over time. Otherwise: use the value of `0` to allow the OS to assign free ports
on demand.

## Test Requirements


1. Test cases should be written so that each class works with exactly one
Slider-deployed cluster
1. Every test MUST have its own cluster name -preferably derived from the
classname.
1. This cluster should be deployed in an `@BeforeClass` method.
1. The `@AfterClass` method MUST tear this cluster down.
1. Tests must skip their execution if functional tests -or the 
specific hbase or accumulo categories- are disabled.
1. Tests within the suite (i.e. class) must be designed to be independent
-to work irrespectively of the ordering of other tests.

## Running and debugging the functional tests.

The functional tests all 

1. In the root `slider` directory, build a complete Slider release

        mvn install -DskipTests
1. Start the YARN cluster/set up proxies to connect to it, etc.

1. In the `slider-funtest` dir, run the test

        mvn test -Dtest=TestHBaseCreateCluster
        
A common mistake during development is to rebuild the `slider-core` JARs
then the `slider-funtest` tests without rebuilding the `slider-assembly`.
In this situation, the tests are in sync with the latest build of the code
-including any bug fixes- but the scripts executed by those tests are
of a previous build of `slider-core.jar`. As a result, the fixes are not picked
up.

#### To propagate changes in slider-core through to the funtest classes for
testing, you must build/install all the slider packages from the root assembly.

    mvn clean install -DskipTests

## Limitations of slider-funtest

1. All tests run from a single client -workload can't scale
1. Output from failed AM and containers aren't collected

## Troubleshooting the functional tests

1. If you are testing in a local VM and stops responding, it'll have been
swapped out to RAM. Rebooting can help, but for a long term fix go through
all the Hadoop configurations (HDFS, YARN, Zookeeper) and set their heaps to
smaller numbers, like 256M each. Also: turn off unused services (hcat, oozie,
webHDFS)

1. The YARN UI will list the cluster launches -look for the one
with a name close to the test and view its logs

1. Container logs will appear "elsewhere". The log lists
the containers used -you may be able to track the logs
down from the specific nodes.

1. If you browse the filesystem, look for the specific test clusters
in `~/.slider/cluster/$testname`

1. If you are using a secure cluster, make sure that the clocks
are synchronized, and that you have a current token -`klist` will
tell you this. In a VM: install and enable `ntp`, consider rebooting if ther
are any problems. Check also that it has the same time zone settings
as the host OS.
