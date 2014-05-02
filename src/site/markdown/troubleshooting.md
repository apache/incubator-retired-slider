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

# Troubleshooting

Slider can be tricky to start using, because it combines the need to set
up a YARN application, with the need to have an HBase configuration
that works


### Common problems

## Classpath for Slider AM wrong

The Slider Application Master, the "Slider AM" builds up its classpath from
those JARs it has locally, and the JARS pre-installed on the classpath

This often surfaces in an exception that can be summarized as
"hadoop-common.jar is not on the classpath":

    Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/hadoop/util/ExitUtil$ExitException
    Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.util.ExitUtil$ExitException
      at java.net.URLClassLoader$1.run(URLClassLoader.java:202)
      at java.security.AccessController.doPrivileged(Native Method)
      at java.net.URLClassLoader.findClass(URLClassLoader.java:190)
      at java.lang.ClassLoader.loadClass(ClassLoader.java:306)
      at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:301)
      at java.lang.ClassLoader.loadClass(ClassLoader.java:247)
    Could not find the main class: org.apache.hadoop.yarn.service.launcher.ServiceLauncher.  Program will exit.


For ambari-managed deployments, we recommend the following

  
      <property>
        <name>yarn.application.classpath</name>
        <value>
          /etc/hadoop/conf,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop-mapreduce/lib/*
        </value>
      </property>

The `yarn-site.xml` file for the site will contain the relevant value.

### Application  Instantiation fails, "TriggerClusterTeardownException: Unstable Cluster" 

Slider gives up if it cannot keep enough instances of a role running -or more
precisely, if they keep failing. 

If this happens on cluster startup, it means that the application is not working

     org.apache.hoya.exceptions.TriggerClusterTeardownException: Unstable Cluster: 
     - failed with role worker failing 4 times (4 in startup); threshold is 2
     - last failure: Failure container_1386872971874_0001_01_000006 on host 192.168.1.86,
       see http://hor12n22.gq1.ygridcore.net:19888/jobhistory/logs/192.168.1.86:45454/container_1386872971874_0001_01_000006/ctx/yarn

This message warns that a role -here worker- is failing to start and it has failed
more than the configured failure threshold is. What it doesn't do is say why it failed,
because that is not something the AM knows -that is a fact hidden in the logs on
the container that failed.

The final bit of the exception message can help you track down the problem,
as it points you to the logs.

In the example above the failure was in `container_1386872971874_0001_01_000006`
on the host `192.168.1.86`. If you go to then node manager on that machine (the YARN
RM web page will let you do this), and look for that container,
you may be able to grab the logs from it. 

A quicker way is to browse to the URL on the next line.
Note: the URL depends on yarn.log.server.url being properly configured.

It is from those logs that the cause of the problem -because they are the actual
output of the actual application which Slider is trying to deploy.



### Not all the containers start -but whenever you kill one, another one comes up.

This is often caused by YARN not having enough capacity in the cluster to start
up the requested set of containers. The AM has submitted a list of container
requests to YARN, but only when an existing container is released or killed
is one of the outstanding requests granted.

Fix #1: Ask for smaller containers

edit the `yarn.memory` option for roles to be smaller: set it 64 for a smaller
YARN allocation. *This does not affect the actual heap size of the 
application component deployed*

Fix #2: Tell YARN to be less strict about memory consumption

Here are the properties in `yarn-site.xml` which we set to allow YARN 
to schedule more role instances than it nominally has room for.

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
  
If you create too many instances, your hosts will start swapping and
performance will collapse -we do not recommend using this in production.


### Configuring YARN for better debugging
 
 
One configuration to aid debugging is tell the nodemanagers to
keep data for a short period after containers finish

    <!-- 10 minutes after a failure to see what is left in the directory-->
    <property>
      <name>yarn.nodemanager.delete.debug-delay-sec</name>
      <value>600</value>
    </property>

You can then retrieve logs by either the web UI, or by connecting to the
server (usually by `ssh`) and retrieve the logs from the log directory


We also recommend making sure that YARN kills processes

    <!--time before the process gets a -9 -->
    <property>
      <name>yarn.nodemanager.sleep-delay-before-sigkill.ms</name>
      <value>30000</value>
    </property>

 
