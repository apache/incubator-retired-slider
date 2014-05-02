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
  
# CLI Actions

 
## Important

1. This document is still being updated from the original hoya design
2. The new cluster model of separated specification files for internal, resource and application configuration
has not been incorporated.
1. What is up to date is the CLI command list and arguments
 
## client configuration
 
As well as the CLI options, the `conf/slider-client.xml` XML file can define arguments used to communicate with the Application instance


####    `fs.defaultFS`

Equivalent to setting the filesystem with `--filesystem`



## Common

### System Properties

Arguments of the form `-S key=value` define JVM system properties.

These are supported primarily to define options needed for some Kerberos configurations.

### Definitions
 
Arguments of the form `-D key=value` define JVM system properties.

These can define client options that are not set in `conf/hoya-client.xml` - or to override them.
 
### Cluster names

All actions that must take an instance name will fail with `EXIT_UNKNOWN_INSTANCE`
if one is not provided.

## Action: Build

Builds a cluster -creates all the on-filesystem datastructures, and generates a cluster description
that is both well-defined and deployable -*but does not actually start the cluster*

    build (instancename,
      options:List[(String,String)],
      components:List[(String, int)],
      componentOptions:List[(String,String, String)],
      resourceOptions:List[(String,String)],
      resourceComponentOptions:List[(String,String, String)],
      confdir: URI,
      provider: String
      zkhosts,
      zkport,
      image
      apphome
      appconfdir
      

#### Preconditions

(Note that the ordering of these preconditions is not guaranteed to remain constant)

The instance name is valid

    if not valid-instance-name(instancename) : raise SliderException(EXIT_COMMAND_ARGUMENT_ERROR)

The instance must not be live. This is purely a safety check as the next test should have the same effect.

    if slider-instance-live(YARN, instancename) : raise SliderException(EXIT_CLUSTER_IN_USE)

The instance must not exist

    if is-dir(HDFS, instance-path(FS, instancename)) : raise SliderException(EXIT_CLUSTER_EXISTS)

The configuration directory must exist it does not have to be the instance's HDFS instance,
as it will be copied there -and must contain only files

    let FS = FileSystem.get(appconfdir)
    if not isDir(FS, appconfdir) raise SliderException(EXIT_COMMAND_ARGUMENT_ERROR)
    forall f in children(FS, appconfdir) :
        if not isFile(f): raise IOException

There's a race condition at build time where between the preconditions being met and the instance specification being saved, the instance
is created by another process. This addressed by creating a lock file, `writelock.json` in the destination directory. If the file
exists, no other process may acquire the lock.

There is a less exclusive readlock file, `readlock.json` which may be created by any process that wishes to read the configuration.
If it exists when another process wishes to access the files, the subsequent process may read the data, but MUST NOT delete it
afterwards. A process attempting to acquire the writelock must check for the existence of this file before AND after creating the
writelock file, failing if its present. This retains a small race condition: a second or later reader may still be reading the data
when a process successfully acquires the write lock. If this proves to be an issue, a stricter model could be implemented, with each reading process creating a unique named readlock- file.




#### Postconditions

All the instance directories exist

    is-dir(HDFS', instance-path(HDFS', instancename))
    is-dir(HDFS', original-conf-path(HDFS', instancename))
    is-dir(HDFS', generated-conf-path(HDFS', instancename))

The application cluster specification saved is well-defined and deployable

    let instance-description = parse(data(HDFS', instance-json-path(HDFS', instancename)))
    well-defined-instance(instance-description)
    deployable-application-instance(HDFS', instance-description)

More precisely: the specification generated before it is saved as JSON is well-defined and deployable; no JSON file will be created
if the validation fails.

Fields in the cluster description have been filled in

    internal.global["internal.provider.name"] == provider
    app_conf.global["zookeeper.port"]  == zkport
    app_conf.global["zookeeper.hosts"]  == zkhosts
    

    package => app_conf.global["agent.package"] = package
    
    

Any `apphome` and `image` properties have propagated

    apphome == null or clusterspec.options["cluster.application.home"] == apphome
    image == null or clusterspec.options["cluster.application.image.path"] == image

(The `well-defined-application-instance()` requirement above defines the valid states
of this pair of options)


All role sizes have been mapped to `component.instances` fields

    forall (name, size) in components :
        resources.components[name]["components.instances"] == size




All option parameters have been added to the `options` map in the specification

    forall (opt, val) in options :
        app_conf.global[opt] == val
        
    forall (opt, val) in resourceOptions :
        resource.global[opt] == val

All component option parameters have been added to the specific components's option map
in the relevant configuration file

    forall (name, opt, val) in componentOptions :
        app_conf.components[name][opt] == val

    forall (name, opt, val) in resourceComponentOptions :
        resourceComponentOptions.components[name][opt] == val

To avoid some confusion as to where keys go, all options beginning with the
prefix `component.` are automatically copied into the resources file:

    forall (opt, val) in options where startswith(opt, "component.") 
            or startswith(opt, "role.") 
            or startswith(opt, "yarn."): 
        resource.global[opt] == val

    forall (name, opt, val) in componentOptions where startswith(opt, "component.") 
            or startswith(opt, "role.") 
            or startswith(opt, "yarn."):
        resourceComponentOptions.components[name][opt] == val
          

There's no explicit rejection of duplicate options, the outcome of that
state is 'undefined'. 

What is defined is that if Slider or its provider provided a default option value,
the command-line supplied option will override it.

All files that were in the configuration directory are now copied into the "original" configuration directory

    let FS = FileSystem.get(appconfdir)
    let dest = original-conf-path(HDFS', instancename)
    forall [c in children(FS, confdir) :
        data(HDFS', dest + [filename(c)]) == data(FS, c)

All files that were in the configuration directory now have equivalents in the generated configuration directory

    let FS = FileSystem.get(appconfdir)
    let dest = generated-conf-path(HDFS', instancename)
    forall [c in children(FS, confdir) :
        isfile(HDFS', dest + [filename(c)])


## Action: Thaw

    thaw <instancename> [--wait <timeout>]

Thaw takes an application instance with configuration and (possibly) data on disk, and
attempts to create a live application with the specified number of nodes

#### Preconditions

    if not valid-instance-name(instancename) : raise SliderException(EXIT_COMMAND_ARGUMENT_ERROR)

The cluster must not be live. This is purely a safety check as the next test should have the same effect.

    if slider-instance-live(YARN, instancename) : raise SliderException(EXIT_CLUSTER_IN_USE)

The cluster must not exist

    if is-dir(HDFS, application-instance-path(FS, instancename)) : raise SliderException(EXIT_CLUSTER_EXISTS)

The cluster specification must exist, be valid and deployable

    if not is-file(HDFS, cluster-json-path(HDFS, instancename)) : SliderException(EXIT_UNKNOWN_INSTANCE)
    if not well-defined-application-instance(HDFS, application-instance-path(HDFS, instancename)) : raise SliderException(EXIT_BAD_CLUSTER_STATE)
    if not deployable-application-instance(HDFS, application-instance-path(HDFS, instancename)) : raise SliderException(EXIT_BAD_CLUSTER_STATE)

### Postconditions


After the thaw has been performed, there is now a queued request in YARN
for the chosen (how?) queue

    YARN'.Queues'[amqueue] = YARN.Queues[amqueue] + [launch("slider", instancename, requirements, context)]

If a wait timeout was specified, the cli waits until the application is considered
running by YARN (the AM is running), the wait timeout has been reached, or
the application has failed

    waittime < 0 or (exists a in slider-running-application-instances(yarn-application-instances(YARN', instancename, user))
        where a.YarnApplicationState == RUNNING)


## Outcome: AM-launched state

Some time after the AM was queued, if the relevant
prerequisites of the launch request are met, the AM will be deployed

#### Preconditions

* The resources referenced in HDFS (still) are accessible by the user
* The requested YARN memory and core requirements could be met on the YARN cluster and 
specific YARN application queue.
* There is sufficient capacity in the YARN cluster to create a container for the AM.

#### Postconditions

Define a YARN state at a specific time `t` as `YARN(t)`; the fact that
an AM is launched afterwards

The AM is deployed if there is some time `t` after the submission time `t0`
where the application is listed 

    exists t1 where t1 > t0 and slider-instance-live(YARN(t1), user, instancename)

At which time there is a container in the cluster hosting the AM -it's
context is the launch context

    exists c in containers(YARN(t1)) where container.context = launch.context

There's no way to determine when this time `t1` will be reached -or if it ever
will -its launch may be postponed due to a lack of resources and/or higher priority
requests using resources as they become available.

For tests on a dedicated YARN cluster, a few tens of seconds appear to be enough
for the AM-launched state to be reached, a failure to occur, or to conclude
that the resource requirements are unsatisfiable.

## Outcome: AM-started state

A (usually short) time after the AM is launched, it should start

* The node hosting the container is working reliably
* The supplied command line could start the process
* the localized resources in the context could be copied to the container (which implies
that they are readable by the user account the AM is running under)
* The combined classpath of YARN, extra JAR files included in the launch context,
and the resources in the slider client 'conf' dir contain all necessary dependencies
to run Slider.
* There's no issue with the cluster specification that causes the AM to exit
with an error code.

Node failures/command line failures are treated by YARN as an AM failure which
will trigger a restart attempt -this may be on the same or a different node.

#### preconditions

The AM was launched at an earlier time, `t1`

    exists t1 where t1 > t0 and am-launched(YARN(t1)


#### Postconditions

The application is actually started if it is listed in the YARN application list
as being in the state `RUNNING`, an RPC port has been registered with YARN (visible as the `rpcPort`
attribute in the YARN Application Report,and that port is servicing RPC requests
from authenticated callers.

    exists t2 where:
        t2 > t1 
        and slider-instance-live(YARN(t2), YARN, instancename, user)
        and slider-live-instances(YARN(t2))[0].rpcPort != 0
        and rpc-connection(slider-live-instances(YARN(t2))[0], HoyaClusterProtocol)

A test for accepting cluster requests is querying the cluster status
with `HoyaClusterProtocol.getJSONClusterStatus()`. If this returns
a parseable cluster description, the AM considers itself live.

## Outcome: Applicaton Instance operational state

Once started, Slider enters the operational state of trying to keep the numbers
of live role instances matching the numbers specified in the cluster specification.

The AM must request the a container for each desired instance of a specific roles of the
application, wait for those requests to be granted, and then instantiate
the specific application roles on the allocated containers.

Such a request is made on startup, whenever a failure occurs, or when the
cluster size is dynamically updated.

The AM releases containers when the cluster size is shrunk during a flex operation,
or during teardown.

### steady state condition

The steady state of a Slider cluster is that the number of live instances of a role,
plus the number of requested instances , minus the number of instances for
which release requests have been made must match that of the desired number.

If the internal state of the Slider AM is defined as `AppState`

    forall r in clusterspec.roles :
        r["component.instances"] ==
          AppState.Roles[r].live + AppState.Roles[r].requested - AppState.Roles[r].released

The `AppState` represents Slider's view of the external YARN system state, based on its
history of notifications received from YARN. 

It is indirectly observable from the cluster state which an AM can be queried for


    forall r in AM.getJSONClusterStatus().roles :
        r["component.instances"] ==
          r["role.actual.instances"] + r["role.requested.instances"] - r["role.releasing.instances"]

Slider does not consider it an error if the number of actual instances remains below
the desired value (i.e. outstanding requests are not being satisfied) -this is
an operational state of the cluster that Slider cannot address.

### Cluster startup

On a healthy dedicated test cluster, the time for the requests to be satisfied is
a few tens of seconds at most: a failure to achieve this state is a sign of a problem.

### Node or process failure

After a container or node failure, a new container for a new instance of that role
is requested.

The failure count is incremented -it can be accessed via the `"role.failed.instances"`
attribute of a role in the status report.

The number of failures of a role is tracked, and used by Slider as to when to
conclude that the role is somehow failing consistently -and it should fail the
entire application.

This has initially been implemented as a simple counter, with the cluster
option: `"hoya.container.failure.threshold"` defining that threshold.

    let status = AM.getJSONClusterStatus() 
    forall r in in status.roles :
        r["role.failed.instances"] < status.options["hoya.container.failure.threshold"]


### Instance startup failure


Startup failures are measured alongside general node failures.

A container is deemed to have failed to start if either of the following conditions
were met:

1. The AM received an `onNodeManagerContainerStartFailed` event.

1. The AM received an `onCompletedNode` event on a node that started less than 
a specified number of seconds earlier -a number given in the cluster option
`"hoya.container.failure.shortlife"`. 

More sophisticated failure handling logic than is currently implemented may treat
startup failures differently from ongoing failures -as they can usually be
treated as a sign that the container is failing to launch the program reliably -
either the generated command line is invalid, or the application is failing
to run/exiting on or nearly immediately.

## Action: Create

Create is simply `build` + `thaw` in sequence  - the postconditions from the first
action are intended to match the preconditions of the second.

## Action: Freeze

    freeze instancename [--wait time] [--message message]

The *freeze* action "freezes" the cluster: all its nodes running in the YARN
cluster are stopped, leaving all the persistent state.

The operation is intended to be idempotent: it is not an error if 
freeze is invoked on an already frozen cluster

#### Preconditions

The cluster name is valid and it matches a known cluster 

    if not valid-instance-name(instancename) : raise SliderException(EXIT_COMMAND_ARGUMENT_ERROR)
    
    if not is-file(HDFS, application-instance-path(HDFS, instancename)) :
        raise SliderException(EXIT_UNKNOWN_INSTANCE)

#### Postconditions

If the cluster was running, an RPC call has been sent to it `stopCluster(message)`

If the `--wait` argument specified a wait time, then the command will block
until the cluster has finished or the wait time was exceeded. 

If the `--message` argument specified a message -it must appear in the
YARN logs as the reason the cluster was frozen.


The outcome should be the same:

    not slider-instance-live(YARN', instancename)

## Action: Flex

Flex the cluster size: add or remove roles. 

    flex instancename 
    components:List[(String, int)]

1. The JSON cluster specification in the filesystem is updated
1. if the cluster is running, it is given the new cluster specification,
which will change the desired steady-state of the application

#### Preconditions

    if not is-file(HDFS, cluster-json-path(HDFS, instancename)) :
        raise SliderException(EXIT_UNKNOWN_INSTANCE)

#### Postconditions

    let originalSpec = data(HDFS, cluster-json-path(HDFS, instancename))
    
    let updatedSpec = originalspec where:
        forall (name, size) in components :
            updatedSpec.roles[name]["component.instances"] == size
    data(HDFS', cluster-json-path(HDFS', instancename)) == updatedSpec
    rpc-connection(slider-live-instances(YARN(t2))[0], HoyaClusterProtocol)
    let flexed = rpc-connection(slider-live-instances(YARN(t2))[0], HoyaClusterProtocol).flexClusterupdatedSpec)


#### AM actions on flex

    boolean HoyaAppMaster.flexCluster(ClusterDescription updatedSpec)
  
If the  cluster is in a state where flexing is possible (i.e. it is not in teardown),
then `AppState` is updated with the new desired role counts. The operation will
return once all requests to add or remove role instances have been queued,
and be `True` iff the desired steady state of the cluster has been changed.

#### Preconditions

      well-defined-application-instance(HDFS, updatedSpec)
  

#### Postconditions

    forall role in AppState.Roles.keys:
        AppState'.Roles'[role].desiredCount = updatedSpec[roles]["component.instances"]
    result = AppState' != AppState


The flexing may change the desired steady state of the cluster, in which
case the relevant requests will have been queued by the completion of the
action. It is not possible to state whether or when the requests will be
satisfied.

## Action: Destroy

Idempotent operation to destroy a frozen cluster -it succeeds if the 
cluster has already been destroyed/is unknown, but not if it is
actually running.

#### Preconditions

    if not valid-instance-name(instancename) : raise SliderException(EXIT_COMMAND_ARGUMENT_ERROR)

    if slider-instance-live(YARN, instancename) : raise SliderException(EXIT_CLUSTER_IN_USE)


#### Postconditions

The cluster directory and all its children do not exist

    not is-dir(HDFS', application-instance-path(HDFS', instancename))
  

## Action: Status

    status instancename [--out outfile]
    2
#### Preconditions

    if not slider-instance-live(YARN, instancename) : raise SliderException(EXIT_UNKNOWN_INSTANCE)

#### Postconditions

The status of the application has been successfully queried and printed out:

    let status = slider-live-instances(YARN).rpcPort.getJSONClusterStatus()
    
if the `outfile` value is not defined then the status appears part of stdout
    
    status in STDOUT'

otherwise, the outfile exists in the local filesystem

    (outfile != "") ==>  data(LocalFS', outfile) == body
    (outfile != "") ==>  body in STDOUT'

## Action: Exists

This probes for a named cluster being defined or actually being in the running
state.

In the running state; it is essentially the status
operation with only the exit code returned

#### Preconditions


    if not is-file(HDFS, application-instance-path(HDFS, instancename)) :
        raise SliderException(EXIT_UNKNOWN_INSTANCE)

#### Postconditions

The operation succeeds if the cluster is running and the RPC call returns the cluster
status.

    if live and not slider-instance-live(YARN, instancename):
      retcode = -1
    else:  
      retcode = 0
 
## Action: getConf

This returns the live client configuration of the cluster -the
site-xml file.

    getconf --format (xml|properties) --out [outfile]

*We may want to think hard about whether this is needed*

#### Preconditions

    if not slider-instance-live(YARN, instancename) : raise SliderException(EXIT_UNKNOWN_INSTANCE)


#### Postconditions

The operation succeeds if the cluster status can be retrieved and saved to 
the named file/printed to stdout in the format chosen

    let status = slider-live-instances(YARN).rpcPort.getJSONClusterStatus()
    let conf = status.clientProperties
    if format == "xml" : 
        let body = status.clientProperties.asXmlDocument()
    else:
        let body = status.clientProperties.asProperties()
        
    if outfile != "" :
        data(LocalFS', outfile) == body
    else
        body in STDOUT'

## Action: list

    list [instancename]

Lists all clusters of a user, or only the one given

#### Preconditions

If a instancename is specified it must be in YARNs list of active or completed applications
of that user:

    if instancename != "" and [] == yarn-application-instances(YARN, instancename, user) 
        raise SliderException(EXIT_UNKNOWN_INSTANCE)


#### Postconditions

If no instancename was given, all hoya applications of that user are listed,
else only the one running (or one of the finished ones)
  
    if instancename == "" :
        forall a in yarn-application-instances(YARN, user) :
            a.toString() in STDOUT'
    else
       let e = yarn-application-instances(YARN, instancename, user) 
       e.toString() in STDOUT'

## Action: killcontainer

This is an operation added for testing. It will kill a container in the cluster
*without flexing the cluster size*. As a result, the cluster will detect the
failure and attempt to recover from the failure by instantiating a new instance
of the cluster

    killcontainer cluster --id container-id
    
#### Preconditions

    if not slider-instance-live(YARN, instancename) : raise SliderException(EXIT_UNKNOWN_INSTANCE)

    exists c in hoya-app-containers(YARN, instancename, user) where c.id == container-id 
    
    let status := AM.getJSONClusterStatus() 
    exists role = status.instances where container-id in status.instances[role].values


#### Postconditions

The container is not in the list of containers in the cluster

    not exists c in containers(YARN) where c.id == container-id 

And implicitly, not in the running containers of that application

    not exists c in hoya-app-containers(YARN', instancename, user) where c.id == container-id 

At some time `t1 > t`, the status of the application (`AM'`) will be updated to reflect
that YARN has notified the AM of the loss of the container

     
    let status' = AM'.getJSONClusterStatus() 
    len(status'.instances[role]) < len(status.instances[role]) 
    status'.roles[role]["role.failed.instances"] == status'.roles[role]["role.failed.instances"]+1


At some time `t2 > t1` in the future, the size of the containers of the application
in the YARN cluster `YARN''` will be as before 

    let status'' = AM''.getJSONClusterStatus() 
    len(status''.instances[r] == len(status.instances[r]) 
