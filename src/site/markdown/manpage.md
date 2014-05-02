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

# slider: YARN-hosted applications

## NAME

slider -YARN-hosted applications

## SYNOPSIS

Slider enables applications to be dynamically created on a YARN-managed datacenter.
The program can be used to create, pause, and shutdown the application. It can also be used to list current active
and existing but not running "frozen" application instances.
 
## CONCEPTS

1. A *Slider application* is an application packaged to be deployed by Slider. It consists of one or more distributed *components* 

1. A *Slider application instance*  is a slider application configured to be deployable on a specific YARN cluster, with a specific configuration. An instance can be *live* -actually running- or *frozen*. When frozen all its configuration details and instance-specific data are preserved on HDFS.

1. An *image* is a *tar.gz* file containing binaries used to create the application.  1. Images are kept in the HDFS filesystem and identified by their path names; filesystem permissions can be used to share images amongst users.

1. An *image configuration* is a directory that is overlaid file-by-file onto the conf/ directory inside the HBase image.

1. Users can have multiple image configurations -they too are kept in HDFS, identified by their path names, and can be shared by setting the appropriate permissions, along with a configuration template file.

1. Only those files provided in the image configuration directory overwrite the default values contained in the image; all other configuration files are retained.

1. Late-binding properties can also be provided at create time.

1. Slider can overwrite some of the configuration properties to enable the dynamically created components to bind correctly to each other.

1. An *instance directory* is a directory created in HDFS describing the application instance; it records the configuration -both user specified, application-default and any dynamically created by slider. 

1. A user can create an application instance.

1. A live instances can be *frozen*, saving its final state to its application instance state directory. All running components are shut down.

1. A frozen instance can be *thawed* -a its components started on or near the servers where they were previously running.

1. A frozen instance can be *destroyed*. 

1. Running instances can be listed. 

1. An instance consists of a set of components

1. The supported component types depends upon the slider application.

1. the count of each component must initially be specified when a application instance is created.

1. Users can flex an application instance: adding or removing components dynamically.
If the application instance is live, the changes will have immediate effect. If not, the changes will be picked up when the instance is next thawed.


<!--- ======================================================================= -->

## Invoking Slider

 
    slider <ACTION> [<name>] [<OPTIONS>]


<!--- ======================================================================= -->

## COMMON COMMAND-LINE OPTIONS

### `--conf configuration.xml`

Configure the Slider client. This allows the filesystem, zookeeper instance and other properties to be picked up from the configuration file, rather than on the command line.

Important: *this configuration file is not propagated to the application. It is purely for configuring the client itself. 

### `-D name=value`

Define a Hadoop configuration option which overrides any options in the client configuration XML files.


### `-m, --manager url`

URL of the YARN resource manager


### `--fs filesystem-uri`

Use the specific filesystem URI as an argument to the operation.





<!--- ======================================================================= -->


<!--- ======================================================================= -->

## Actions

COMMANDS



### `build <name>`

Build an instance of the given name, with the specific options.

It is not started; this can be done later with a `thaw` command.

### `create <name>`

Build and run an applicaton instance of the given name 

The `--wait` parameter, if provided, specifies the time to wait until the YARN application is actually running. Even after the YARN application has started, there may be some delay for the instance to start up.

### Arguments for `build` and `create` 



##### `--package <uri-to-package>`  

This define the slider application package to be deployed.


##### `--option <name> <value>`  

Set a application instance option. 

Example:

Set an option to be passed into the `-site.xml` file of the target system, reducing
the HDFS replication factor to 2. (

    --option site.dfs.blocksize 128m
    
Increase the number of YARN containers which must fail before the Slider application instance
itself fails.
    
    -O slider.container.failure.threshold 16

##### `--appconf dfspath`

A URI path to the configuration directory containing the template application specification. The path must be on a filesystem visible to all nodes in the YARN cluster.

1. Only one configuration directory can be specified.
1. The contents of the directory will only be read when the application instance is created/built.

Example:

    --appconf hdfs://namenode/users/slider/conf/hbase-template
    --appconf file://users/accumulo/conf/template



##### `--apphome localpath`

A path to the home dir of a pre-installed application. If set when a Slider
application instance is created, the instance will run with the binaries pre-installed
on the nodes at this location

*Important*: this is a path in the local filesystem which must be present
on all hosts in the YARN cluster

Example

    --apphome /usr/hadoop/hbase

##### `--template <filename>`

Filename for the template application instance configuration. This
will be merged with -and can overwrite- the built-in configuration options, and can
then be overwritten by any command line `--option` and `--compopt` arguments to
generate the final application configuration.

##### `--resources <filename>`

Filename for the resources configuration. This
will be merged with -and can overwrite- the built-in resource options, and can
then be overwritten by any command line `--resopt`, `--rescompopt` and `--component`
arguments to generate the final resource configuration.


##### `--image path`

The full path in Hadoop HDFS  to a `.tar` or `.tar.gz` file containing 
the binaries needed to run the target application.

Example

    --image hdfs://namenode/shared/binaries/hbase-0.96.tar.gz

##### `--component <name> <count>`

The desired number of instances of a component


Example

    --component worker 16

This just sets the `component.instances` value of the named component's resource configuration.
it is exactly equivalent to 

	--rco worker component.instances 16



#### `--compopt <component> <option> <value>` 

Provide a specific application configuration option for a component

Example

    --compopt master env.TIMEOUT 10000

These options are saved into the `app_conf.json` file; they are not used to configure the YARN Resource
allocations, which must use the `--rco` parameter

#### Resource Component option `--rescompopt` `--rco`

`--rescompopt <component> <option> <value>` 

Set any role-specific option, such as its YARN memory requirements.

Example

    --rco worker master yarn.memory 2048
    --rco worker worker yarn.memory max


##### `--zkhosts host1:port1,[host2:port2,host3:port3, ...] `

The zookeeper quorum.

Example

    --zkhosts zk1:2181,zk2:2181,zk3:4096

If unset, the zookeeper quorum defined in the property `slider.zookeeper.quorum`
is used

### `destroy <name>`

Destroy a (stopped) applicaton instance .

Important: This deletes all persistent data

Example

    slider destroy instance1

### `exists <name> [--live]`

Probe the existence of the named Slider application instance. If the `--live` flag is set, the instance
must actually be running

If not, an error code is returned.

When the --live` flag is unset, the command looks for the application instance to be
defined in the filesystem -its operation state is not checked.

Return codes

     0 : application instance is defined in the filesystem
    70 : application instance is unknown

Example:

    slider exists instance4

#### Live Tests

When the `--live` flag is set, the application instance must be running for the command
to succeed

1. The probe does not check the status of any Slider-deployed services, merely that a application instance has been deployed
1. A application instance that is finished or failed is not considered to be live.

Return codes

     0 : application instance is running
    -1 : application instance exists but is not running
    70 : application instance is unknown


Example:

    slider exists instance4 --live

### `flex <name> [--component component count]* `

Flex the number of workers in an application instance to the new value. If greater than before, new copies of the component will be requested. If less, component instances will be destroyed.

This operation has a return value of 0 if the size of a running instance was changed. 

It returns -1 if there is no running application instance, or the size of the flexed instance matches that of the original -in which case its state does not change.

Example

    slider flex instance1 --component worker 8 --filesystem hdfs://host:port
    slider flex instance1 --component master 2 --filesystem hdfs://host:port
    

### `freeze <name>  [--force] [--wait time] [--message text]`

freeze the application instance. The running application is stopped. Its settings are retained in HDFS.

The `--wait` argument can specify a time in seconds to wait for the application instance to be frozen.

The `--force` flag causes YARN asked directly to terminate the application instance. 
The `--message` argument supplies an optional text message to be used in
the request: this will appear in the application's diagnostics in the YARN RM UI.

If an unknown (or already frozen) application instance is named, no error is returned.

Examples

    slider freeze instance1 --wait 30
    slider freeze instance2 --force --message "maintenance session"


### `list <name>`

List running Slider application instances visible to the user.

If an instance name is given and there is no running instance with that name, an error is returned. 

Example

    slider list
    slider list instance1

### `status <name> [--out <filename>]`

Get the status of the named application instance in JSON format. A filename can be used to 
specify the destination.

Examples:

    slider status instance1 --manager host:port
    
    slider status instance2 --manager host:port --out status.json



### `thaw <name> [--wait time`]

Resume a frozen application instance, recreating it from its previously saved state. This will include a best-effort attempt to create the same number of nodes as before, though their locations may be different.

Examples:

    slider thaw instance2
    slider thaw instance1 --wait 60


If the application instance is already running, this command will not affect it.


### `version`

The command `slider version` prints out information about the compiled
Slider application, the version of Hadoop against which it was built -and
the version of Hadoop that is currently on its classpath.

Note that this is the client-side Hadoop version, not that running on the server, though
that can be obtained in the status operation



## Commands for testing


These are clearly abnormal operations; they are here primarily for testing
-and documented for completeness.

### `kill-container <name> --id container-id`

Kill a  YARN container belong to the application. This is useful primarily for testing the 
resilience to failures.

Container IDs can be determined from the application instance status JSON document.


### `am-suicide <name> [--exitcode code] [--message message] [--wait time]`

This operation is purely for testing Slider Application Master restart;
it triggers an asynchronous self-destruct operation in the AM -an 
operation that does not make any attempt to cleanly shut down the process. 

If the application has not exceeded its restart limit (as set by
`slider.yarn.restart.limit`), YARN will attempt to restart the failed application.

Example

    slider am-suicide --exitcode 1 --wait 5000 -message "test"

<!--- ======================================================================= -->


## Instance Naming

Application instance names must:

1. be at least one character long
1. begin with a lower case letter
1. All other characters must be in the range \[a-z,0-9,_]
1. All upper case characters are converted to lower case
 
Example valid names:

    slider1
    storm4
    hbase_instance
    accumulo_m1_tserve4

