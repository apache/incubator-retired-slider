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

# Hoya Cluster Specification

A Hoya Cluster Specification is a JSON file which describes a cluster to
Hoya: what application is to be deployed, which archive file contains the
application, specific cluster-wide options, and options for the individual
roles in a cluster.

##  Hoya Options

These are options read by the Hoya Application Master; the program deployed
in the YARN cluster to start all the other roles.

When the AM is started, all entries in the cluster-wide  are loaded as Hadoop
configuration values.

Only those related to Hadoop, the filesystem in use, YARN and hoya will be
used; others are likely to be ignored.

## Cluster Options

Cluster wide options are used to configure the application itself.

These are specified at the command line with the `-O key=value` syntax

All options beginning with the prefix `site.` are converted into 
site XML options for the specific application (assuming the application uses 
a site XML configuration file)

Standard keys are defined in the class `org.apache.hoya.api.OptionKeys`.

####  `hoya.test`

A boolean value to indicate this is a test run, not a production run. In this
mode Hoya opts to fail fast, rather than retry container deployments when
they fail. It is primarily used for internal tests.


#### `hoya.container.failure.shortlife`

An integer stating the time in milliseconds before which a failed container is
considered 'short lived'.

A failure of a short-lived container is treated as a sign of a problem with
the role configuration and/or another aspect of the Hoya cluster -or
a problem with the specific node on which the attempt to run
the container was made.



#### `hoya.container.failure.threshold`

An integer stating the number of failures tolerated in a single role before
the cluster is considered to have failed.



#### `hoya.am.monitoring.enabled`

A boolean flag to indicate whether application-specific health monitoring
should take place. Until this monitoring is completed the option
is ignored -it is added as `false` by default on clusters created.


## Roles

A Hoya application consists of the Hoya Application Master, "the AM", which
manages the cluster, and a number of instances of the "roles" of the actual
application.

For HBase the roles are `master` and `worker`; Accumulo has more.

For every role, the cluster specification can define
1. How many instances of that role are desired.
1. Some options with well known names for configuring the runtimes
of the roles.
1. Environment variables needed to help configure and run the process.
1. Options for YARN

### Standard Role Options

#### Desired instance count `role.instances`

#### Additional command-line arguments `role.additional.args`

This argument is meant to provide a configuration-based static option
that is provided to every instance of the given role. For example, this is
useful in providing a binding address to Accumulo's Monitor process.

Users can override the option on the hoya executable using the roleopt argument:

    --roleopt monitor role.additional.args "--address 127.0.0.1"

#### YARN container memory `yarn.memory`

The amount of memory in MB for the YARN container hosting
that role. Default "256".

The special value `max` indicates that Hoya should request the
maximum value that YARN allows containers to have -a value
determined dynamically after the cluster starts.

Examples:

    --roleopt master yarn.memory 2048
    --roleopt worker yarn.memory max

If a YARN cluster is configured to set process memory limits via the OS,
and the application tries to use more memory than allocated, it will fail
with the exit code "143". 

#### YARN vCores `yarn.vcores`

Number of "Cores" for the container hosting
a role. Default value: "1"

The special value `max` indicates that Hoya should request the
maximum value that YARN allows containers to have -a value
determined dynamically after the cluster starts.

As well as being able to specify the numeric values of memory and cores
in role, via the `--roleopt` argument, you can now ask for the maximum
allowed value by using the parameter `max`

Examples:

    --roleopt master yarn.vcores 2
    --roleopt master yarn.vcores max

####  Master node web port `app.infoport`

The TCP socket port number to use for the master node web UI. This is translated
into an application-specific site.xml property for both Accumulo and HBase.

If set to a number other than the default, "0", then if the given port is in
use, the role instance will not start. This will occur if YARN is already
running a master node on that server, or if another application is using
the same TCP port.

#### JVM Heapsize `jvm.heapsize`

Heapsize as a JVM option string, such as `"256M"` or `"2G"`

    --roleopt worker jvm.heapsize 8G

This is not correlated with the YARN memory -changes in the YARN memory allocation
are not reflected in the JVM heapsize -and vice versa.

### Environment variables
 
 
All role options beginning with `env.` are automatically converted to
environment variables which will be set for all instances of that role.

    --roleopt worker env.MALLOC_ARENA 4

## Accumulo-Specific Options

### Accumulo cluster options

Here are options specific to Accumulo clusters.

####  Mandatory: Zookeeper Home: `zk.home`

Location of Zookeeper on the target machine. This is needed by the 
Accumulo startup scripts.

#### Mandatory: Hadoop Home `hadoop.home`

Location of Hadoop on the target machine. This is needed by the 
Accumulo startup scripts.

#### Mandatory: Accumulo database password  `accumulo.password`

This is the password used to control access to the accumulo data.
A random password (from a UUID, hence very low-entropy) is chosen when
the cluster is created. A more rigorous password can be set on the command
line _at the time of cluster creation_.


## Hoya AM Role Options

The Hoya Application Master has its own role, `hoya`, which can also
be configured with role options. Currently only JVM and YARN options 
are supported:

    --roleopt hoya jvm.heapsize 256M
    --roleopt hoya jvm.opts "-Djvm.property=true"
    --roleopt hoya yarn.memory 512

Normal memory requirements of the AM are low, except in the special case of
starting an accumulo cluster for the first time. In this case, `bin\accumulo init`
needs to be run: the extra memory requirements of the accumulo process
need to be included in the hoya role's `yarn.memory` values.
