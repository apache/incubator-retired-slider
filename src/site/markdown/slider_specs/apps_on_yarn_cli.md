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

#Slider CLI

This document describes the CLI to deploy and manage YARN applications using Slider.

## Operations

### `create <application> <--app.package packagelocation> <--resource resourcespec> <--app.instance.configuration appconfiguration> <--options sliderconfiguration> [--provider providername]`

Build an application specification by laying out the application artifacts in HDFS and prepares it for *start*. This involves specifying the application name, application package, YARN resource requirements, application specific configuration overrides, options for Slider, and optionally declaring the provider, etc. The provider is invoked during the build process, and can set default values for components.

The default application configuration and components would be built from the application metadata contained in the package. *create* performs a structural validation of the application package and validates the supplied resources specification and instance configuration against the application package.

**parameters**

* application: name of the application instance, must be unique within the Hadoop cluster
* packagelocation: the application package on local disk or HDFS
* resourcespec: YARN resource requirements for application components
* appconfiguration: configuration override for the application
* sliderconfiguration: configuration for Slider itself such as location of YARN resource manager, HDFS file system, ZooKeeper quorom nodes, etc.
* providername: name of the any non-default provider. Agent provider is default.

### `destroy <application>` 

destroy a (stopped) application. The stop check is there to prevent accidentally destroying an application in use

### `start <application>` 

Start an application instance that is already created through *create*

### `stop <application>  [--force]`

Stop the application instance. 

The --force operation tells YARN to kill the application instance without involving the AppMaster.

### `flex <application> [--component componentname count]* <--resource resourcespec>`

Update component instance count
 
### `configure <application> <--app.instance.configuration appconfiguration>`
 
Modify application instance configuration. Updated configuration is only applied after the application is restarted.

### `status <application>`

Report the status of an application instance. If there is a record of an application instance in a failed/finished state AND there is no live application instance, the finished application is reported. Otherwise, the running application's status is reported.

If there a no instances of an application in the YARN history, the application is looked up in the applications directory, and the status is listed if present.


### `listapplications [--accepted] [--started] [--live] [--finished] [--failed] [--stopped]` 

List all applications, optionally the ones in the named specific states. 


### `getconfig <application> [--config filename  [--dir destdir|--outfile destfile]]`

list/retrieve any configs published by the application.

if no --config option is provided all available configs are listed

If a --file is specified, it is downloaded to the current directory with the specified filename, unless a destination directory/filename is provided


### `history <application>`

Lists all life-cycle events of the application instance since the last *start*

### `kill --containers [containers] --components [components] --nodes [nodes]`

Kill listed containers, everything in specific components, or on specific nodes. This can be used to trigger restart of services and decommission of nodes

### `wait <application> [started|live|stopped] --timeout <time>`

Block waiting for a application to enter the specififed state. Can fail if the application stops while waiting for it to be started/live
