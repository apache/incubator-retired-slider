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

# Cluster Specification

### Notation: 

In this document, a full path to a value is represented as a path 
`options/zookeeper.port`  ; an assigment as  `options/zookeeper.port=2181`.

A wildcard indicates all entries matching a path: `options/zookeeper.*`
or `/roles/*/yarn.memory`


## History

The Hoya cluster specification was implicitly defined in the file
`org.apache.hoya.api.ClusterDescription`. It had a number of roles

1. Persistent representaton of cluster state
1. Internal model of desired cluster state within the Application Master.
1. Dynamic representation of current cluster state when the AM
was queried, marshalled over the network as JSON.
1. Description of updated state when reconfiguring a running cluster.

Initially the dynamic status included a complete history of all containers
-this soon highlit some restrictions on the maximum size of a JSON-formatted
string in Hadoop's "classic" RPC: 32K, after which the string was silently
truncated. Accordingly, this history was dropped.

Having moved to Protocol Buffers as the IPC wire format, with a web view
alongside, this history could be reconsidered.

The initial design place most values into the root entry, and relied
on Jaxon introspection to set and retrieve the values -it was a
Java-first specification, with no external specificatin or regression tests.

As the number of entries in the root increased, the design switched to storing
more attributes into specific sections *under* the root path:

* `info`: read-only information about the cluster.
* `statistics`: Numeric statistics about the cluster

# Sections

## Root

Contains various string and integer values

    "version": "1.0",
    "name": "test_cluster_lifecycle",
    "type": "hbase",
    "state": 3,
    "createTime": 1393512091276,
    "updateTime": 1393512117286,
    "originConfigurationPath": "hdfs://sandbox.hortonworks.com:8020/user/stevel/.hoya/cluster/test_cluster_lifecycle/snapshot",
    "generatedConfigurationPath": "hdfs://sandbox.hortonworks.com:8020/user/stevel/.hoya/cluster/test_cluster_lifecycle/generated",
    "dataPath": "hdfs://sandbox.hortonworks.com:8020/user/stevel/.hoya/cluster/test_cluster_lifecycle/database",


* `version`: version of the JSON file. Not currently used
to validate version compatibility; at this point in time
releases may not be able to read existing .json files.

* `name`: cluster name
* `type`: reference to the provider type -this triggers a Hadoop configuration
property lookup to find the implementation classes.
* `state`: an enumeration value of the cluster state.

        int STATE_INCOMPLETE = 0;
        int STATE_SUBMITTED = 1;
        int STATE_CREATED = 2;
        int STATE_LIVE = 3;
        int STATE_STOPPED = 4;
        int STATE_DESTROYED = 5;
        
  Only two states are persisted, "incomplete" and "created", though more
  are used internally.
  The `incomplete` state is used during cluster create/build,
   allowing an incomplete JSON file to be written
  -so minimising the window for race conditions on cluster construction.
        
* `createTime` and `updateTime`: timestamps, informative only.
 The `createTime` value is duplicated in `/info/createTimeMillis`
* `originConfigurationPath`, `generatedConfigurationPath`, `dataPath` paths
used internally -if changed the cluster may not start.

*Proposed*: 
1. Move all state bar `name` and cluster state
into a section `/hoya-internal`.
1. The cluster state is moved from an enum to a simple
 boolean, `valid`, set to true when the cluster JSON
 has been fully constructed.

## `/info`

Read-only list of information about the application. Generally this is
intended to be used for debugging and testing.

### Persisted values: static information about the file history
 
    "info" : {
      "create.hadoop.deployed.info" : "(detached from release-2.3.0) @dfe46336fbc6a044bc124392ec06b85",
      "create.application.build.info" : "Hoya Core-0.13.0-SNAPSHOT Built against commit# 1a94ee4aa1 on Java 1.7.0_45 by stevel",
      "create.hadoop.build.info" : "2.3.0",
      "create.time.millis" : "1393512091276",
    },
 
*Proposed*: move persisted info K-V pairs to a section `/diagnostics`.
 
### Dynamic values: 
 
 
 whether the AM supports service restart without killing all the containers hosting
 the role instances:
 
    "hoya.am.restart.supported" : "false",
    
 timestamps of the cluster going live, and when the status query was made
    
    "live.time" : "27 Feb 2014 14:41:56 GMT",
    "live.time.millis" : "1393512116881",
    "status.time" : "27 Feb 2014 14:42:08 GMT",
    "status.time.millis" : "1393512128726",
    
  yarn data provided to the AM
    
    "yarn.vcores" : "32",
    "yarn.memory" : "2048",
  
  information about the application and hadoop versions in use. Here
  the application was built using Hadoop 2.3.0, but is running against the version
  of Hadoop built for HDP-2.
  
    "status.application.build.info" : "Hoya Core-0.13.0-SNAPSHOT Built against commit# 1a94ee4aa1 on Java 1.7.0_45 by stevel",
    "status.hadoop.build.info" : "2.3.0",
    "status.hadoop.deployed.info" : "bigwheel-m16-2.2.0 @704f1e463ebc4fb89353011407e965"
 
 
 ## `instances`
 
 Information about the live containers in a cluster

     "instances": {
       "hoya": [ "container_1393511571284_0002_01_000001" ],
       "master": [ "container_1393511571284_0002_01_000003" ],
       "worker": [ 
         "container_1393511571284_0002_01_000002",
         "container_1393511571284_0002_01_000004"
       ]
     },

There's no information about location, nor is there any history about containers
that are no longer part of the cluster (i.e. failed & released containers). 

It could be possible to include a list of previous containers,
though Hoya would need to be selective about how many to store
(or how much detail to retain) on those previous containers.

Perhaps the list could be allowed to grow without limit, but detail
only preserved on the last 100. If more containers fail than that,
there is likely to be a problem which the most recent containers
will also display.

*Proposed* 

1. Return to the full serialization of container state -but only for running containers.
1. Have a list of failed containers, but only include last 8; make it a rolling
buffer. This avoids a significantly failing role to overload the status document.

 
 ## `statistics`
 
 Statistics on each role. 
 
 They can be divided into counters that only increase

    "containers.start.completed": 0,
    "containers.start.failed": 0,
    "containers.failed": 0,
    "containers.completed": 0,
    "containers.requested": 0

and those that vary depending upon the current state

    "containers.live": 0,
    "containers.active.requests": 0,
    "containers.desired": 0,


* Propose: move these values out of statistics into some other section, as they
are state, not statistics*


       "statistics": {
         "worker": {
           "containers.start.completed": 0,
           "containers.live": 2,
           "containers.start.failed": 0,
           "containers.active.requests": 0,
           "containers.failed": 0,
           "containers.completed": 0,
           "containers.desired": 2,
           "containers.requested": 0
         },
         "hoya": {
           "containers.unknown.completed": 0,
           "containers.start.completed": 3,
           "containers.live": 1,
           "containers.start.failed": 0,
           "containers.failed": 0,
           "containers.completed": 0,
           "containers.surplus": 0
         },
         "master": {
           "containers.start.completed": 0,
           "containers.live": 1,
           "containers.start.failed": 0,
           "containers.active.requests": 0,
           "containers.failed": 0,
           "containers.completed": 0,
           "containers.desired": 1,
           "containers.requested": 0
         }
       },
    
The `/statistics/hoya` section is unusual in that it provides the aggregate statistics
of the cluster -this is not obvious. A different name could be used -but
again, there's a risk of clash with or confusion with a role. 

Better to have a specific `/statistics/cluster` element, 
and to move the roles' statistics under `/statistics/roles`:

    "statistics": {
      "cluster": {
        "containers.unknown.completed": 0,
        "containers.start.completed": 3,
        "containers.live": 1,
        "containers.start.failed": 0,
        "containers.failed": 0,
        "containers.completed": 0,
        "containers.surplus": 0
  
      },
      "roles": {
        "worker": {
          "containers.start.completed": 0,
          "containers.live": 2,
          "containers.start.failed": 0,
          "containers.active.requests": 0,
          "containers.failed": 0,
          "containers.completed": 0,
          "containers.desired": 2,
          "containers.requested": 0
        },
        "master": {
          "containers.start.completed": 0,
          "containers.live": 1,
          "containers.start.failed": 0,
          "containers.active.requests": 0,
          "containers.failed": 0,
          "containers.completed": 0,
          "containers.desired": 1,
          "containers.requested": 0
        }
      }
    },

This approach allows extra statistics sections to be added (perhaps
by providers), without any changes to the toplevel section.

## Options

A list of options used by Hoya and its providers to build up the AM
and the configurations of the deployed service components


    "options": {
      "zookeeper.port": "2181",
      "site.hbase.master.startup.retainassign": "true",
      "hoya.cluster.application.image.path": "hdfs://sandbox.hortonworks.com:8020/hbase.tar.gz",
      "site.fs.defaultFS": "hdfs://sandbox.hortonworks.com:8020",
      "hoya.container.failure.threshold": "5",
      "site.fs.default.name": "hdfs://sandbox.hortonworks.com:8020",
      "slider.cluster.directory.permissions": "0770",
      "hoya.am.monitoring.enabled": "false",
      "zookeeper.path": "/yarnapps_hoya_stevel_test_cluster_lifecycle",
      "hoya.tmp.dir": "hdfs://sandbox.hortonworks.com:8020/user/stevel/.hoya/cluster/test_cluster_lifecycle/tmp/am",
      "slider.data.directory.permissions": "0770",
      "zookeeper.hosts": "sandbox",
      "hoya.container.failure.shortlife": "60"
    },
  
Some for these options have been created by hoya itself ("hoya.tmp.dir")
for internal use -and are cluster specific. If/when the ability to use
an existing json file as a template for a new cluster is added, having these
options in the configuration will create problems


# Proposed Changes


## Move Hoya internal state to `/hoya-internal`

Move all hoya "private" data to an internal section,`/hoya-internal`
including those in the toplevel directory and in `/options`
  
## Allow `/options` and `roles/*/` options entries to take the value "null".

This would be a definition that the value must be defined before the cluster
can start. Provider templates could declare this.
  
## Make client configuration retrieval hierarchical -and maybe move out of the
status

The current design assumes that it is a -site.xml file being served up. This
does not work for alternate file formats generated by the Provider.

## Role Options

The `/roles/$ROLENAME/` clauses each provide options for a
specific role.

This includes
1. `role.instances`: defines the number of instances of a role to create
1. `env.` environment variables for launching the container
1. `yarn.` properties to configure YARN requests.
1. `jvm.heapsize`: an option supported by some providers to 
fix the heap size of a component.
1. `app.infoport`: an option supported by some providers (e.g. HBase)
to fix the port to which a role (master or worker) binds its web UI.



      "worker": {
        "yarn.memory": "768",
        "env.MALLOC_ARENA_MAX": "4",
        "role.instances": "0",
        "role.name": "worker",
        "jvm.heapsize": "512M",
        "yarn.vcores": "1",
        "app.infoport": "0"
      },

In a live cluster, the role information also includes status information
about the cluster.

      "master": {
        "yarn.memory": "1024",
        "env.MALLOC_ARENA_MAX": "4",
        "role.instances": "0",
        "role.requested.instances": "0",
        "role.name": "master",
        "role.failed.starting.instances": "0",
        "role.actual.instances": "0",
        "jvm.heapsize": "512M",
        "yarn.vcores": "1",
        "role.releasing.instances": "0",
        "role.failed.instances": "0",
        "app.infoport": "0"
      }

The role `hoya` represents the Hoya Application Master itself.

      
      "hoya": {
        "yarn.memory": "256",
        "env.MALLOC_ARENA_MAX": "4",
        "role.instances": "1",
        "role.name": "hoya",
        "jvm.heapsize": "256M",
        "yarn.vcores": "1",
      },

### Proposed: 
1. move all dynamic role status to its own clauses.
1. use a simple inheritance model from `/options`
1. don't allow role entries to alter the cluster state. 
  
### Proposed:  `/clientProperties` continues return Key-val pairs

The `/clientProperties` section will remain, with key-val pairs of type
string, the expectation being this is where providers can insert specific
single attributes for client applications.

These values can be converted to application-specific files on the client,
in code -as done today in the Hoya CLI-, or via template expansion (beyond
the scope of this document.



### Proposed: alongside `/clientProperties`  comes `/clientfiles` 

This section will list all files that an application instance can generate
for clients, along with with a description.

    "/clientfiles/hbase-site.xml": "site information for HBase"
    "/clientfiles/log4.properties": "log4.property file"

A new CLI command would be added to retrieve a client file.
1. The specific file must be named.
1. If it is not present, an error must be raised.
1. If it is present, it is downloaded and output to the console/to a named
destination file/directory `--outfile <file>` and `--outdir <dir>`
1. If the `--list` argument is provided, the list of available files is
returned (e.g.) 

    hbase-site.xml: site information for HBase
    log4.properties: log4.property file
    
*No attempt to parse/process the body of the messages will be returned.*

In a REST implementation of the client API, /clientconf would be a path
to the list of options; each file a path underneath.

Client configuration file retrieval outside the status completely;
the status just lists the possible values; a separate call returns them.

This will  permit binary content to be retrieved, and avoid any marshalling
problems and inefficiencies.

With this change, there will now be two ways to generate client configuration
files

* Client-side: as today
* Server-side: via the provider

Client side is more extensible as it allows for arbitrary clients; server-side
is restricted to those files which the application provider is capable of
generating. The advantage of the server-side option is that for those files
about which the provider is aware of, they will be visible through the 
REST and Web UIs, so trivially retrieved.

### Stop intermixing role specification with role current state

Create a new section, `rolestatus`, which lists the current status
of the roles: how many are running vs requested, how many are being
released.

There's some overlap here with the `/statistics` field, so we should
either merge them or clearly separate the two. Only the `role.failed`
properties match entries in the statistics -perhaps they should be cut.

#### provider-specific status

Allow providers to publish information to the status, in their
own section.

There already is support for providers updating the cluster status
in Hoya 12.1 and earlier, but it has flaws

A key one is that it is done sychronously on a `getStatus()` call;
as providers may perform a live query of their status (example, the HBase
provider looks up the Web UI ports published by HBase to zookeeper),
there's overhead, and if the operation blocks (example: when HBase hasn't
ever been deployed and the zookeeper path is empty), then the status
call blocks.

*Proposed:*

1. There is a specific `/provider` section
1. There's no restriction on what JSON is permitted in this section.
1. Providers may make their own updates to the application state to read and
write this block -operations that are asynchronous to any status queries.
