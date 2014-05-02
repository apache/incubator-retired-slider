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

# Building Slider


Here's how to set this up.

## Before you begin

### Networking

The network on the development system must be functional, with hostname lookup
of the local host working. Tests will fail without this.

### Maven

You will need a version of Maven 3.0+, set up with enough memory

    MAVEN_OPTS=-Xms256m -Xmx512m -Djava.awt.headless=true


*Important*: As of October 6, 2013, Maven 3.1 is not supported due to
[version issues](https://cwiki.apache.org/confluence/display/MAVEN/AetherClassNotFound).

### Protoc

You need a copy of the `protoc`  compile

1. OS/X: `brew install protobuf`
1. Others: consult (Building Hadoop documentation)[http://wiki.apache.org/hadoop/HowToContribute].

The version of protoc installed must be the same as that used by Hadoop itself.
This is absolutely critical to prevent JAR version problems.

## Building a compatible Hadoop version


Slider is built against Hadoop 2 -you can download and install
a copy from the [Apache Hadoop Web Site](http://hadoop.apache.org).


During development, its convenient (but not mandatory)
to have a local version of Hadoop -so that we can find and fix bugs/add features in
Hadoop as well in Slider.


To build and install locally, check out apache svn/github, branch `release-2.4.0`,
and create a branch off that tag

    git clone git://git.apache.org/hadoop-common.git 
    cd hadoop-common
    git remote rename origin apache
    git fetch --tags apache
    git checkout release-2.4.0 -- 
    git checkout -b release-2.4.0


For the scripts below, set the `HADOOP_VERSION` variable to the version

    export HADOOP_VERSION=2.4.0
    
or, for building against a pre-release version of Hadoop 2.4
 
    git checkout branch-2
    export HADOOP_VERSION=2.4.0-SNAPSHOT

To build and install it locally, skipping the tests:

    mvn clean install -DskipTests

To make a tarball for use in test runs:

    #On  osx
    mvn clean install package -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip=true 
    
    # on linux
    mvn clean package -Pdist -Pnative -Dtar -DskipTests -Dmaven.javadoc.skip=true 

Then expand this

    pushd hadoop-dist/target/
    gunzip hadoop-$HADOOP_VERSION.tar.gz 
    tar -xvf hadoop-$HADOOP_VERSION.tar 
    popd

This creates an expanded version of Hadoop. You can now actually run Hadoop
from this directory. Do note that unless you have the native code built for
your target platform, Hadoop will be slower. 

## building a compatible HBase version

If you need to build a version of HBase -rather than use a released version,
here are the instructions (for the hbase-0.98 release branch)

Checkout the HBase `trunk` branch from apache svn/github.  

    
    git clone git://git.apache.org/hbase.git
    cd hbase
    git remote rename origin apache
    git fetch --tags apache

then

    git checkout -b apache/0.98
or

    git checkout tags/0.98.1
    
If you have already been building versions of HBase, remove the existing
set of artifacts for safety:

    rm -rf ~/.m2/repository/org/apache/hbase/
    
The maven command for building hbase artifacts against this hadoop version is 

    mvn clean install assembly:single -DskipTests -Dmaven.javadoc.skip=true

To use a different version of Hadoop from that defined in the `hadoop-two.version`
property of`/pom.xml`:

    mvn clean install assembly:single -DskipTests -Dmaven.javadoc.skip=true -Dhadoop-two.version=$HADOOP_VERSION

This will create an hbase `tar.gz` file in the directory `hbase-assembly/target/`
in the hbase source tree. 

    export HBASE_VERSION=0.98.1
    
    pushd hbase-assembly/target
    gunzip hbase-$HBASE_VERSION-bin.tar.gz 
    tar -xvf hbase-$HBASE_VERSION-bin.tar
    gzip hbase-$HBASE_VERSION-bin.tar
    popd

This will create an untarred directory containing
hbase. Both the `.tar.gz` and untarred file are needed for testing. Most
tests just work directly with the untarred file as it saves time uploading
and downloading then expanding the file.

(and if you set `HBASE_VERSION` to something else, you can pick up that version
-making sure that slider is in sync)

For more information (including recommended Maven memory configuration options),
see [HBase building](http://hbase.apache.org/book/build.html)

For building just the JAR files:

    mvn clean install -DskipTests -Dhadoop.profile=2.0 -Dhadoop-two.version=$HADOOP_VERSION

*Tip:* you can force set a version in Maven by having it update all the POMs:

    mvn versions:set -DnewVersion=0.98.1-SNAPSHOT

## Building Accumulo

Clone accumulo from apache;

    git clone http://git-wip-us.apache.org/repos/asf/accumulo.git


Check out branch 1.5.1-SNAPSHOT



In the accumulo project directory, build it

    mvn clean install -Passemble -DskipTests -Dmaven.javadoc.skip=true \
     -Dhadoop.profile=2 
     
The default Hadoop version for accumulo-1.5.1 is hadoop 2.4.0; to build
against a different version use the command
     
    mvn clean install -Passemble -DskipTests -Dmaven.javadoc.skip=true \
     -Dhadoop.profile=2  -Dhadoop.version=$HADOOP_VERSION

This creates an accumulo tar.gz file in `assemble/target/`. Unzip then untar
this, to create a .tar file and an expanded directory

    accumulo/assemble/target/accumulo-1.5.1-SNAPSHOT-bin.tar
    
 This can be done with the command sequence
    
    export ACCUMULO_VERSION=1.5.1-SNAPSHOT
    
    pushd assemble/target/
    gunzip -f accumulo-$ACCUMULO_VERSION-bin.tar.gz 
    tar -xvf accumulo-$ACCUMULO_VERSION-bin.tar 
    popd
    
Note that the final location of the accumulo files is needed for the configuration,
it may be directly under target/ or it may be in a subdirectory, with 
a path such as `target/accumulo-$ACCUMULO_VERSION-dev/accumulo-$ACCUMULO_VERSION/`


## Testing

### Configuring Slider to locate the relevant artifacts

You must have the file `src/test/resources/slider-test.xml` (this
is ignored by git), declaring where HBase, accumulo, Hadoop and zookeeper are:

    <configuration>
    
      <property>
        <name>slider.test.hbase.home</name>
        <value>/home/slider/hbase/hbase-assembly/target/hbase-0.98.0-SNAPSHOT</value>
        <description>HBASE Home</description>
      </property>
    
      <property>
        <name>slider.test.hbase.tar</name>
        <value>/home/hoya/hbase/hbase-assembly/target/hbase-0.98.0-SNAPSHOT-bin.tar.gz</value>
        <description>HBASE archive URI</description>
      </property> 
         
      <property>
        <name>slider.test.accumulo.home</name>
        <value>/home/hoya/accumulo/assemble/target/accumulo-1.5.1-SNAPSHOT/</value>
        <description>Accumulo Home</description>
      </property>
    
      <property>
        <name>slider.test.accumulo.tar</name>
        <value>/home/hoya/accumulo/assemble/target/accumulo-1.5.1-SNAPSHOT-bin.tar.gz</value>
        <description>Accumulo archive URI</description>
      </property>
      
      <property>
        <name>zk.home</name>
        <value>
          /home/hoya/Apps/zookeeper</value>
        <description>Zookeeper home dir on target systems</description>
      </property>
    
      <property>
        <name>hadoop.home</name>
        <value>
          /home/hoya/hadoop-common/hadoop-dist/target/hadoop-2.3.0</value>
        <description>Hadoop home dir on target systems</description>
      </property>
      
    </configuration>
    

## Debugging a failing test

1. Locate the directory `target/$TESTNAME` where TESTNAME is the name of the 
test case and or test method. This directory contains the Mini YARN Cluster
logs. For example, `TestLiveRegionService` stores its data under 
`target/TestLiveRegionService`

1. Look under that directory for `-logdir` directories, then an application
and container containing logs. There may be more than node being simulated;
every node manager creates its own logdir.

1. Look for the `out.txt` and `err.txt` files for stdout and stderr log output.

1. Slider uses SLF4J to log to `out.txt`; remotely executed processes may use
either stream for logging

Example:

    target/TestLiveRegionService/TestLiveRegionService-logDir-nm-1_0/application_1376095770244_0001/container_1376095770244_0001_01_000001/out.txt

1. The actual test log from JUnit itself goes to the console and into 
`target/surefire/`; this shows the events happening in the YARN services as well
 as (if configured) HDFS and Zookeeper. It is noisy -everything after the *teardown*
 message happens during cluster teardown, after the test itself has been completed.
 Exceptions and messages here can generally be ignored.
 
This is all a bit complicated -debugging is simpler if a single test is run at a
time, which is straightforward

    mvn clean test -Dtest=TestLiveRegionService


### Building the JAR file

You can create the JAR file and set up its directories with

     mvn package -DskipTests

# Development Notes


## Git branch model

The git branch model uses is
[Git Flow](http://nvie.com/posts/a-successful-git-branching-model/).

This is a common workflow model for Git, and built in to
[Atlassian Source Tree](http://sourcetreeapp.com/).
 
The command line `git-flow` tool is easy to install 
 
    brew install git-flow
 
or

    apt-get install git-flow
 
You should then work on all significant features in their own branch and
merge them back in when they are ready.

 
    # until we get a public JIRA we're just using an in-house one. sorry
    git flow feature start BUG-8192
    
    # finishes merges back in to develop/
    git flow feature finish BUG-8192
    
    # release branch
    git flow release start 0.4.0
    
    git flow release finish 0.4.0
    
## Attn OS/X developers

YARN on OS/X doesn't terminate subprocesses the way it does on Linux, so
HBase Region Servers created by the hbase shell script remain running
even after the tests terminate.

This causes some tests -especially those related to flexing down- to fail, 
and test reruns may be very confused. If ever a test fails because there
are too many region servers running, this is the likely cause

After every test run: do a `jps -v` to look for any leftover HBase services
-and kill them.

Here is a handy bash command to do this

    jps -l | grep HRegion | awk '{print $1}' | xargs kill -9


## Groovy 

Slider uses Groovy 2.x as its language for writing tests -for better assertions
and easier handling of lists and closures. Although the first prototype
used Groovy on the production source, this was dropped in favor of
a Java-only production codebase.

## Maven utils


Here are some handy aliases to make maven easier 

    alias mci='mvn clean install -DskipTests'
    alias mi='mvn install -DskipTests'
    alias mvct='mvn clean test'
    alias mvnsite='mvn site:site -Dmaven.javadoc.skip=true'
    alias mvt='mvn test'


