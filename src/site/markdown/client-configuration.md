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

# Client Configuration

This document covers how the client application is itself configured.

## Summary

The client application can be configured

1. On the command line, which can set client options and JVM system properties.
2. With Hadoop-style configuration options in the file `slider-client.xml`
 in the configuration directory`conf/` dir
2. Or, if the environment variable `SLIDER_CONF_DIR` is set, in the
 file `$SLIDER_CONF_DIR/slider-client.xml`
1. Logging is defined in the `log4j.properties` file in the same configuration
directory.
1. VM options can be defined in `SLIDER_JVM_OPTS`

The options defined in a Slider cluster configuration are only used by the client
when creating a cluster -not for the actual client itself.

## Introduction

The Slider client needs to be configured to talk to a Hadoop filesystem and a
YARN resource manager ("the RM"). In a secure cluster it needs to be told the Kerberos
identity, the *principal* of both the HDFS namenode and the YARN RM -and it may
also need some JVM options set in order for Java's Kerberos module to
correctly identify itself to these services.

It cannot rely on local `HADOOP_PREFIX/conf/hadoop-site.xml` and
`$YARN_PREFIX/conf/yarn-site.xml` files -because it is designed to
work on client machines that may not have Hadoop and YARN installed.

Instead all client-side (non-JVM) options can be predefined in the
configuration file `slider-client.xml`. 

## Setting Slider JVM options

Core JVM options can be set in the environment variable `SLIDER_JVM_OPTS`;
if unset the `bin/slider` script will use the default values that were
current when that version of Slider was released. These values may change
across versions, and may in fact be.

At the time of writing, the default values were:

    "-Djava.net.preferIPv4Stack=true -Djava.awt.headless=true -Xmx256m -Dslider.confdir=${confdir}"

To allow some Java system properties to be set without editing this
environment variable, such system properties may be set on the Slider command
line through the `-S` parameter. For example, the following two operations are
equivalent in terms of setting the system property `java.security.krb5.realm`
to the value `LOCAL`.

    export SLIDER_JVM_OPTS="-Djava.security.krb5.realm=LOCAL"

and

    slider -S java.security.krb5.realm=LOCAL

Note that the first declaration invalidates all default JVM options; if any of
those were desired, they should be included in the new definition.

Multiple system property declarations are allowed on the command line -including
duplicate declarations. In such a case the order of assignment is undefined.

For any system property that the user expects to have to issue on every command
-including any kerberos-related properties, adding them to the JVM options
environment variable guarantees that they are always set.

## Setting Slider client options on the command line with the `-D` parameter

The slider client is configured via Hadoop-style configuration options. 
To be precise, all standard Hadoop-common, hadoop-hdfs client and hadoop-yar
client-side options control how Slider communicates with the Hadoop YARN cluster.

There are extra options specific to Slider itself, options which
are again set as Hadoop configuration parameters.

All Hadoop and Slider options can be set on the command line using the `-D`
parameter followed by the appropriate `key=value` argument


For example, here is a definition of the default Hadoop filesystem:

    -D fs.defaultFS=hdfs://namenode:9000
    
Multiple definitions are of course allowed on the command line    
 
    -D fs.defaultFS=hdfs://namenode:9000 -D dfs.namenode.kerberos.principal=hdfs/namenode@LOCAL

Slider-specific options can be made the same way

    -D slider.kerberos.principal=

If duplicate declarations are made the order of assignment is undefined.

# Setting common options through specific command-line arguments

Some Hadoop and Slider options are so common that they have specific
shortcut commands to aid their use

`-m`, `--manager` : sets the YARN resource manager. Equivalent to setting the 
`yarn.resourcemanager.address` option

`--fs`,  `--filesystem`: defines the filesystem. Equivalent to setting the
`fs.defaultFS` option

If these shortcuts are used and the options are also defined via `-D`
declarations, the order of assignment is undefined.
    
# Defining Hadoop and Slider Options in the `slider-client.xml` file.

In the Slider installation, alongside the `bin/slider` script is
a configuration directory `conf`. This contains the files:

1. `log4j.properties`
1. `slider-client.xml`

The `log4j.properties` file is not covered here -it is a standard Log4J file.
At the time of writing, this log configuration file is used on both the
client and the server.

The `slider-client.xml` file is a hadoop-formatted XML options file, which
is read by the Slider client -but not by they Slider Application Master.

Here is an example file:

    <property>
      <name>yarn.resourcemanager.address</name>
      <value>namenode:8033</value>
    </property>
    
    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://namenode:9000</value>
    </property>
 
    <property>
      <name>ipc.client.fallback-to-simple-auth-allowed</name>
      <value>false</value>
    </property>


This defines both the filesystem and the YARN RM, and so obviates the need
to declare either on the command line.

If an option is defined in the `slider-client.xml` file and on the command line
-be it by a `-D key=value` declaration or a `--manager` or `--filesystem` 
definition. (this holds even if the value is declared with `<final>true</final>`).

## Selecting an alternate Slider configuration directory

The environment variable `SLIDER_CONF_DIR` can be used to declare an alternate
configuration directory. If set, the directory it identifies will be used
as the source of the `log4j.properties` and `slider-client.xml` files.

## Slider Client Configuration options

As well as standard YARN and Hadoop configuration options, Slider supports
a limited number of slider-specific configuration parameters.

    <property>
      <name>slider.zookeeper.quorum</name>
      <value>localhost:2181,zookeeper2:4545</value>
    </property>
    
    <property>
      <name>slider.yarn.queue</name>
      <value>default</value>
    </property>
    
    <property>
      <name>slider.security.enabled</name>
      <value>false</value>
    </property>
    
    <property>
      <name>slider.yarn.queue</name>
      <value>default</value>
    </property>

    <property>
      <name>slider.yarn.queue.priority</name>
      <value>1</value>
    </property>

    <property>
      <name>slider.yarn.restart.limit</name>
      <value>5</value>
      <description>How many times to start/restart the Slider AM</description>
    </property>
    
    <property>
      <name>slider.cluster.directory.permissions</name>
      <value>750</value>
    </property>
    
    <property>
      <name>slider.data.directory.permissions</name>
      <value>750</value>
    </property>

### `slider.zookeeper.quorum` - the zookeeper quorum.

This defines the zookeeper quorum for this YARN cluster. 

It is used to locate the service registry, enable running instances to publish
information about their application, and for clients to query this. 

It is also used as the default zookeeper binding for any application that
uses zookeeper in its configuration -the value set when the application is
defined will be copied into the instance definition file.

### `"slider.registry.path"` - the zookeeper path for the service registry

This declares the the zookeeper path for the service registry. 

### `slider.security.enabled` - enable security.

This turns security on; consult [Security](security.html) for more information.


### `slider.yarn.restart.limit` - set limit on Application Master Restarts

This limits how many times YARN should start a failed application master.

A short restart limit is useful when initially creating a cluster, as it
ensures that YARN does not repeatedly try to restart a failing application.

In production, however, a large number prevents YARN from halting a Slider
application merely because failures in the underlying YARN cluster have
triggered restarts.

*Important:* The cluster-wide limit of `yarn.resourcemanager.am.max-attempts`
places an upper limit on the number of retries that any application can request.
If the application fails after less restarts than requested, check this cluster
setting.

### `slider.yarn.queue` - the name of the YARN queue for the cluster.

This identifies the queue submit the application creation request to, which can
define the priority, resource limits and other values of an application. All
containers created in the Slider cluster will share this same queue.

Default value: `default`.

### `slider.yarn.queue.priority` - the name of the YARN queue for the cluster.

This identifies the priority within the queue. The lower the value, the higher the
priority

Default value: `1`.

    bin/slider thaw cl1 -D slider.yarn.queue.priority=5



#### `slider.cluster.directory.permissions`

An octal-format (`chmod`-style) permissions mask for the directory
that contains the cluster specification `${user.home}/.slider/clusters/${clustername}`

    <property>
      <name>slider.cluster.directory.permissions</name>
      <value>750</value>
    </property>

#### `slider.data.directory.permissions`

An octal-format (`chmod`-style) permissions mask for the directory
that contains the application data `${user.home}/.slider/clusters/${clustername}/database`

    <property>
      <name>slider.data.directory.permissions</name>
      <value>750</value>
    </property>


## Debugging configuration issues

If the slider packages are set to log at debug level in the log4j configuration
file, details on properties will be part of the copious output.


## How client options are passed down to created clusters.

Apart from the filesystem bindings, Client configuration options are
not passed down to the XML site specification of the created cluster.

The sole options passed down are the HDFS bindings: `fs.defaultFS`,
which is passed down both as that property and as `fs.default.name`,
and, in a secure cluster, the security flag (`slider.security.enabled`)
and the HDFS Kerberos principal.

