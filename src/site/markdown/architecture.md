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

# Architecture

## Summary

Slider is a YARN application to deploy non-YARN-enabled applications in a YARN cluster

Slider consists of a YARN application master, the "Slider AM", and a client application which communicates with YARN and the Slider AM via remote procedure calls and/or REST requests. The client application offers command line access, as well as low-level API access for test purposes

The deployed application must be a program that can be run across a pool of
YARN-managed servers, dynamically locating its peers. It is not Slider's
responsibility to configure up the peer servers, apart from some initial
application-specific application instance configuration. (The full requirements
of an application are [described in another document](app_needs.md).

Every application instance is described as a set of one or more *component*; each
component can have a different program/command, and a different set of configuration
options and parameters.

The AM takes the details on which roles to start, and requests a YARN container
for each component; It then monitors the state of the application instance, receiving messages
from YARN when a remotely executed process finishes. It then deploys another instance of 
that component.


## Slider Packaging

A key goal of Slider is to support the deployment of existing applications into
a YARN application instance, without having to extend Slider itself. 



## AM Architecture

The application master consists of

 1. The AM engine which handles all integration with external services, specifically YARN and any Slider clients
 1. A *provider* specific to deploying a class of applications.
 1. The Application State. 

The Application State is the model of the application instance, containing

 1. A specification of the desired state of the application instance -the number of instances of each role, their YARN and process memory requirements and some other options. 
 1. A map of the current instances of each role across the YARN cluster, including reliability statistics of each node in the application instance used.
 1. [The Role History](rolehistory.html) -a record of which nodes roles were deployed on for re-requesting the same nodes in future. This is persisted to disk and re-read if present, for faster application startup times.
 1. Queues of track outstanding requests, released and starting nodes

The Application Engine integrates with the outside world: the YARN Resource Manager ("the RM"), and the node-specific Node Managers, receiving events from the services, requesting or releasing containers via the RM,  and starting applications on assigned containers.

After any notification of a change in the state of the cluster (or an update to the client-supplied cluster specification), the Application Engine passes the information on to the Application State class, which updates its state and then returns a list of cluster operations to be submitted: requests for containers of different types -potentially on specified nodes, or requests to release containers.

As those requests are met and allocation messages passed to the Application Engine, it works with the Application State to assign them to specific components, then invokes the provider to build up the launch context for that application.

The provider has the task of populating  container requests with the file references, environment variables and commands needed to start the provider's supported programs.  

The core provider deploys a minimal agent on the target containers, then, as the agent checks in to the agent provider's REST API, executes commands issued to it. 

The set of commands this agent executes focuses on downloading archives from HDFS, expanding them, then running Python scripts which perform the
actual configuration and execution of the target problem -primarily through template expansion.


To summarize: Slider is not an classic YARN analysis application, which allocates and schedules work across the cluster in short-to-medium life containers with the lifespan of a query or an analytics session, but instead for an application with a lifespan of days to months. Slider works to keep the actual state of its application cluster to match the desired state, while the application has the tasks of recovering from node failure, locating peer nodes and working with data in an HDFS filesystem. 

As such it is one of the first applications designed to use YARN as a platform for long-lived services -Samza being the other key example. These application's  needs of YARN are different, and their application manager design is focused around maintaining the distributed application in its desired state rather than the ongoing progress of submitted work.

The clean model-view-controller split was implemented to isolate the model and aid mock testing of large clusters with simulated scale, and hence increase confidence that Slider can scale to work in large YARN clusters and with larger application instances. 



### Failure Model

The application master is designed to be a [crash-only application](https://www.usenix.org/legacy/events/hotos03/tech/full_papers/candea/candea.pdf), clients are free to terminate
the application instance by asking YARN directly. 

There is an RPC call to stop the application instance - this is a nicety which includes a message in the termination log, and
could, in future, perhaps warn the provider that the application instance is being torn down. That is a potentially dangerous feature
to add -as provider implementors may start to expect the method to be called reliably. Slider is designed to fail without
warning, to rebuild its state on a YARN-initiated restart, and to be manually terminated without any advance notice.

### RPC Interface


The RPC interface allows the client to query the current application state, and to update it by pushing out a new JSON specification. 

The core operations are

* `getJSONClusterStatus()`: get the status of the application instance as a JSON document.
* `flexCluster()` update the desired count of role instances in the running application instance.
* `stopCluster` stop the application instance

There are some other low-level operations for extra diagnostics and testing, but they are of limited importancs 

The `flexCluster()` call takes a JSON application instance specification and forwards it to the AM -which extracts the desired counts of each role to update the Application State. A change in the desired size of the application instance, is treated as any reported failure of node:
it triggers a re-evaluation of the application state, building up the list of container add and release requests to make of
the YARN resource manager.

The final operation, `stopCluster()`, stops the application instance. 

### Security and Identity

Slider's security model is described in detail in [an accompanying document](security.html)

A Slider application instance is expected to access data belonging to the user creating the instance. 

In a secure YARN cluster, this is done by acquiring Kerberos tokens in the client when the application instance is updated, tokens which
are propagated to the Slider AM and thence to the deployed application containers themselves. These
tokens are valid for a finite time period. 

HBase has always required keytab files to be installed on every node in the Hadoop for it to have secure access -this requirement
holds for Slider-deployed HBase clusters. Slider does not itself adopt the responsibility of preparing or distributing these files;
this must be done via another channel.

In Hadoop 2.2, the tokens for communication between the Slider AM and YARN expire after -by default- 72 hours. The
HDFS tokens will also expire after some time period. This places an upper bound on the lifespan of a Slider application (or any
other long-lived YARN application) in a secure Hadoop cluster. 



In an insecure Hadoopp cluster, the Slider AM and its containers are likely to run in a different OS account from the submitting user.
To enable access to the database files as that submitting use, the identity of the user is provided when the AM is created; the
AM will pass this same identity down to the created containers. This information *identifies* the user -but does not *authenticate* them: they are trusted to be who they claim to be.

 
