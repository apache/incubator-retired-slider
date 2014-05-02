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
  
# Formal Slider Model

This is the model of Slider and YARN for the rest of the specification.

## File System

A File System `HDFS` represents a Hadoop FileSystem -either HDFS or another File
System which spans the cluster. There are also other filesystems that
can act as sources of data that is then copied into HDFS. These will be marked
as `FS` or with the generic `FileSystem` type.


There's ongoing work in [HADOOP-9361](https://issues.apache.org/jira/browse/HADOOP-9361)
to define the Hadoop Filesytem APIs using the same notation as here,
the latest version being available on [github](https://github.com/steveloughran/hadoop-trunk/tree/stevel/HADOOP-9361-filesystem-contract/hadoop-common-project/hadoop-common/src/site/markdown/filesystem)
Two key references are

 1. [The notation reused in the Slider specifications](https://github.com/steveloughran/hadoop-trunk/blob/stevel/HADOOP-9361-filesystem-contract/hadoop-common-project/hadoop-common/src/site/markdown/filesystem/notation.md)
 1. [The model of the filesystem](https://github.com/steveloughran/hadoop-trunk/blob/stevel/HADOOP-9361-filesystem-contract/hadoop-common-project/hadoop-common/src/site/markdown/filesystem/model.md)
 
 The model and its predicates and invariants will be used in these specifications.
 
## YARN

From the perspective of YARN application, The YARN runtime is a state, `YARN`, 
comprised of: ` (Apps, Queues, Nodes)`

    Apps: Map[AppId, ApplicationReport]
    
An application has a name, an application report and a list of outstanding requests
    
    App: (Name, report: ApplicationReport, Requests:List[AmRequest])

An application report contains a mixture of static and dynamic state of the application
and the AM.

    ApplicationReport: AppId, Type, User, YarnApplicationState, AmContainer, RpcPort, TrackingURL,

YARN applications have a number of states. These are ordered such that if the
`state.ordinal() > RUNNING.ordinal()  ` then the application has entered an exit state.
 
    YarnApplicationState : [NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED ]
  
AMs can request containers to be added or released    

    AmRequest = { add-container(priority, requirements), release(containerId)}

Job queues are named queues of job requests; there is always a queue called `"default"`

    Queues: Map[String:Queue]
        Queue:  List[Requests]
        Request = {
          launch(app-name, app-type, requirements, context)
        }
        Context: (localized-resources: Map[String,URL], command)


This is doesn't completely model the cluster from the AM perspective -there's no
notion of node operations (launching code in a container) or events coming from YARN.

The `Nodes` structure models the nodes in a cluster

    Nodes:  Map[nodeID,(name, containers:List[Container])] 

A container contains some state

    Container: (containerId, appId, context)

The containers in a cluster are the aggregate set of all containers across
all nodes

    def containers(YARN) =
        [c for n in keys(YARN.Nodes) for c in YARN.Nodes[n].Containers ]


The containers of an application are all containers that are considered owned by it,

    def app-containers(YARN, appId: AppId) =
        [c in containers(YARN) where c.appId == appId ]

### Operations & predicates used the specifications


    def applications(YARN, type) = 
        [ app.report for app in YARN.Apps.values where app.report.Type == type]
    
    def user-applications(YARN, type, user)
        [a in applications(YARN, type) where: a.User == user]
    

## UserGroupInformation

Applications are launched and executed on hosts computers: either client machines
or nodes in the cluster, these have their own state which may need modeling

    HostState: Map[String, String]

A key part of the host state is actually the identity of the current user,
which is used to define the location of the persistent state of the cluster -including
its data, and the identity under which a deployed container executes.

In a secure cluster, this identity is accompanied by kerberos tokens that grant the caller
access to the filesystem and to parts of YARN itself.

This specification does not currently explicitly model the username and credentials.
If it did they would be used throughout the specification to bind to a YARN or HDFS instance.

`UserGroupInformation.getCurrentUser(): UserGroupInformation`

Returns the current user information. This information is immutable and fixed for the duration of the process.



## Slider Model

### Cluster name

A valid cluster name is a name of length > 1 which follows the internet hostname scheme of letter followed by letter or digit

    def valid-cluster-name(c) =
        len(c)> 0
        and c[0] in ['a'..'z']
        and c[1] in (['a'..'z'] + ['-'] + ['0..9']) 

### Persistent Cluster State

A Slider cluster's persistent state is stored in a path

    def cluster-path(FS, clustername) = user-home(FS) + ["clusters", clustername]
    def cluster-json-path(FS, clustername) = cluster-path(FS, clustername) + ["cluster.json"]
    def original-conf-path(FS, clustername) = cluster-path(FS, clustername) + ["original"] 
    def generated-conf-path(FS, clustername) = cluster-path(FS, clustername) + ["generated"]
    def data-path(FS, clustername) = cluster-path(FS, clustername) + ["data"]

When a cluster is built/created the specified original configuration directory
is copied to `original-conf-path(FS, clustername)`; this is patched for the
specific instance bindings and saved into `generated-conf-path(FS, clustername)`.

A cluster *exists* if all of these paths are found:

    def cluster-exists(FS, clustername) =
        is-dir(FS, cluster-path(FS, clustername))
        and is-file(FS, cluster-json-path(FS, clustername))
        and is-dir(FS, original-conf-path(FS, clustername))
        and generated-conf-path(FS, original-conf-path(FS, clustername))

A cluster is considered `running` if there is a Slider application type belonging to the current user in one of the states
`{NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING}`. 

    def final-yarn-states = {FINISHED, FAILED, KILLED }

    def slider-app-instances(YARN, clustername, user) =
        [a in user-applications(YARN, "slider", user) where:
             and a.Name == clustername]
             
    def slider-app-running-instances(YARN, clustername, user) =
        [a in slider-app-instances(YARN, user, clustername) where:
             not a.YarnApplicationState in final-yarn-state]
    
    def slider-app-running(YARN, clustername, user) =
        [] != slider-app-running-instances(YARN, clustername, user) 
        
    def slider-app-live-instances(YARN, clustername, user) =
        [a in slider-app-instances(YARN, user, clustername) where:
             a.YarnApplicationState == RUNNING]
             
    def slider-app-live(YARN, clustername, user) =
       [] != slider-app-live-instances(YARN, clustername, user) 

### Invariant: there must never be more than one running instance of a named Slider cluster


There must never be more than one instance of the same Slider cluster running:

    forall a in user-applications(YARN, "slider", user):
        len(slider-app-running-instances(YARN, a.Name, user)) <= 1

There may be multiple instances in a finished state, and one running instance alongside multiple finished instances -the applications
that work with Slider MUST select a running cluster ahead of any terminated clusters.

### Containers of an application 

     
The containers of a slider application are the set of containers of that application

    def slider-app-containers(YARN, clustername, user) =
      app-containers(YARN, appid where
        appid = slider-app-running-instances(YARN, clustername, user)[0])




### RPC Access to a slider cluster


 An application is accepting RPC requests for a given protocol if there is a port binding
 defined and it is possible to authenticate a connection using the specified protocol

     def rpc-connection(appReport, protocol) =
         appReport.host != null 
         appReport.rpcPort != 0 
         and RPC.getProtocolProxy(appReport.host, appReport.rpcPort, protocol)

 Being able to open an RPC port is the strongest definition of liveness possible
 to make: if the AM responds to RPC operations, it is doing useful work.

### Valid Cluster Description

The `cluster.json` file of a cluster configures Slider to deploy the application. 

#### well-defined-cluster(cluster-description)

A Cluster Description is well-defined if it is valid JSON and required properties are present

**OBSOLETE**


Irrespective of specific details for deploying the Slider AM or any provider-specific role instances,
a Cluster Description defined in a `cluster.json` file at the path `cluster-json-path(FS, clustername)`
is well-defined if

1. It is parseable by the jackson JSON parser.
1. Root elements required of a Slider cluster specification must be defined, and, where appropriate, non-empty
1. It contains the extensible elements required of a Slider cluster specification. For example, `options` and `roles`
1. The types of the extensible elements match those expected by Slider.
1. The `version` element matches a supported version
1. Exactly one of `options/cluster.application.home` and `options/cluster.application.image.path` must exist.
1. Any cluster options that are required to be integers must be integers

This specification is very vague here to avoid duplication: the cluster description structure is currently implicitly defined in 
`org.apache.slider.api.ClusterDescription` 

Currently Slider ignores unknown elements during parsing. This may be changed.

The test for this state does not refer to the cluster filesystem

#### deployable-cluster(FS, cluster-description)

A  Cluster Description defines a deployable cluster if it is well-defined cluster and the contents contain valid information to deploy a cluster

This defines how a cluster description is valid in the extends the valid configuration with 

* The entry `name` must match a supported provider
* Any elements that name the cluster match the cluster name as defined by the path to the cluster:

        originConfigurationPath == original-conf-path(FS, clustername)
        generatedConfigurationPath == generated-conf-path(FS, clustername)
        dataPath == data-path(FS, clustername)

* The paths defined in `originConfigurationPath` , `generatedConfigurationPath` and `dataPath` must all exist.
* `options/zookeeper.path` must be defined and refer to a path in the ZK cluster
defined by (`options/zookeeper.hosts`, `zookeeper.port)` to which the user has write access (required by HBase and Accumulo)
* If `options/cluster.application.image.path` is defined, it must exist and be readable by the user.
* It must declare a type that maps to a provider entry in the Slider client's XML configuration:

        len(clusterspec["type"]) > 0 
        clientconfig["slider.provider."+ clusterspec["type"]] != null

* That entry must map to a class on the classpath which can be instantiated
and cast to `HoyaProviderFactory`.

        let classname = clientconfig["slider.provider."+ clusterspec["type"]] 
        (Class.forName(classname).newInstance()) instanceof HoyaProviderFactory 

#### valid-for-provider(cluster-description, provider)

A provider considers a specification valid if its own validation logic is satisfied. This normally
consists of rules about the number of instances of different roles; it may include other logic.

