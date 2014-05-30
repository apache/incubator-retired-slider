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

# A YARN Service Registry for Apache Slider

## April 2014

# Introduction

This document looks at the needs and options of a service registry.

The core issue is that as the location(s) of a dynamically deployed application are unknown, the standard Hadoop and Java configuration model of some form of text files containing hostnames, ports and URLS no longer works. You cannot define up-front where a service will be.

Some Hadoop applications -HBase and Accumulo -have solved this with custom ZK bindings. This works for the specific clients, but requires hbase and accumulo client JARs in order to be able to work with the content. (or a re-implementation with knowledge of the non-standard contents of the ZK nodes)

Other YARN applications will need to publish their bindings - this includes, but is not limited to- Slider deployed applications. Again, these applications can use their own registration and binding model, which would again require custom clients to locate the registry information and parse the contents.

YARN provides some minimal publishing of AM remote endpoints: a URL to what is assumed to be a Web UI (not a REST API), and an IPC port. The URL is displayed in the YARN UI -in which case it is accessed via a proxy which (currently) only support HTTP GET operations. The YARN API call to list all applications can be used to locate a named instance of an application by (user, application-type, name), and then obtain the raw URL and IPC endpoints. This enumeration process is an O(apps) operation on the YARN RM and only provides access to those two endpoints. Even with the RAW URL, REST operations have proven "troublesome", due to a web filter which redirects all direct requests to the proxy -unless it comes from the same host as the proxy.

Hadoop client applications tend to retrieve all their configuration information from files in the local filesystem, hadoop-site.xml, hdfs-site.xml, hbase-site.xml, etc. This requires the configuration files to be present on all systems. Tools such as Ambari can keep the files in the server up to date -assuming a low rate of change- ---but these tools do nothing for the client applications themselves. It is up to the cluster clients to (somehow) retrieve these files, and to keep their copies up to date. *This is a problem that exists with today's applications*. 

As an example, if a YARN client does not know the value of "yarn.application.classpath", it cannot successfully deploy any application in the YARN cluster which needs the cluster-side Hadoop and YARN JARs on its application master's classpath. This is not a theoretical problem, as some clusters have a different classpath from the default: without a correct value the Slider AM does not start. And, as it is designed to be run remotely, it cannot rely on a local installation of YARN to provide the correct values ([YARN-973](https://issues.apache.org/jira/browse/YARN-973)).

# What do we need?

**Discovery**: An IPC and URL discovery system for service-aware applications to use to look up a service to which it wishes to talk to. This is not an ORB -it's not doing redirection -, but it is something that needs to be used before starting IPC or REST communications. 

**Configuration**: A way for clients of a service to retrieve more configuration data than simply the service endpoints. For example: everything needed to create a site.xml document.

## Client-side

* Allow clients of a YARN application to locate the service instance and its service ports (web, IPC, REST...) efficiently even on a large YARN cluster. 

* Allow clients to retrieve configuration values which can be processed client-side into the configuration files and options which the application needs

* Give clients confidence that the service with which they interact is the one they expect to interact with -not another potentially malicious service deployed by a different user. 

* clients to be able to watch a service and retrieve notification of changes

* cross-language support.

## For all Services

* Allow services to publish their binding details for the AM and of code running in the containers (which may be published by the containers)

* Use entries in registry as a way of enforcing uniqueness of the instance (app, owner, name)? 

* values to update when a service is restarted on a different host

* values to indicate when a service is not running. This may be implicit "no entry found" or explicit "service exists but not running"

* Services to be able to act as clients to other services

## For Slider Services (and presumably others)

* Ability to publish information about configuration documents that can be retrieved -and URLs

* Ability to publish facts internal to the application (e.g. agent reporting URLs)

* Ability to use service paths as a way to ensure a single instance of a named service can be deployed by a user

## Management and PaaS UIs

* Retrieve lists of web UI URLs of AM and of deployed components

* Enum components and their status

* retrieve dynamic assignments of IPC ports

* retrieve dynamic assignments of JMX ports

* retrieve any health URLs for regular probes

* Listen to changes in the service mix -the arrival and departure of service instances, as well as changes in their contents.



## Other Needs

* Registry-configured applications. In-cluster applications should be able to subscribe to part of the registry
to pick up changes that affect them -both for their own application configuration, and for details about
applications on which they depend themselves.

* Knox: get URLs that need to be converted into remote paths

* Cloud-based deployments: work on virtual infrastructures where hostnames are unpredictable.

# Open Source Registry code

What can we use to implement this from ASF and ASF-compatible code? 

## Zookeeper

We'd need a good reason not to use this. There are still some issues

1. Limits on amount of published data?

2. Load limits, especially during cluster startup, or if a 500-mapper job all wants to do a lookup.

3. Security story

4. Impact of other ZK load on the behaviour of the service registry -will it cause problems if overloaded -and are they recoverable?

## Apache Curator

Netflix's core curator -now [Apache Curator](http://curator.apache.org/)- framework adds a lot to make working with ZK easier, including pluggable retry policies, binding tools and other things.

There is also its "experimental" [service discovery framework](http://curator.apache.org/curator-x-discovery-server/index.html), which

1. Allows a service to register a URL with a name and unique ID (and custom metadata). multiple services of a given name can be registered

2. Allows a service to register >1 URL.

3. Has a service client which performs lookup and can cache results.

4. Has a REST API

Limitations

* The service discovery web UI and client does not work with the version of
Jackson (1.8.8) in Hadoop 2.4. The upgraded version in Hadoop 2.5 is compatible [HADOOP-10104](https://issues.apache.org/jira/browse/HADOOP-10104).

* The per-entry configuration payload attempts to get jason to perform Object/JSON mapping with the classname provided as an attribute in the JSON. This destroys all ability of arbitrary applications to parse the published data, as well as cross-language clients -is brittle and morally wrong from a data-sharing perspective.

    {
    
      "name" : "name",
      "id" : "service",
      "address" : "localhost",
      "port" : 8080,
      "sslPort" : 443,
      "payload" : {
        "@class" : "org.apache.slider.core.registry.ServiceInstanceData",
        "externalView" : {
          "key" : "value"
        }
      },
      "registrationTimeUTC" : 1397249829062,
      "serviceType" : "DYNAMIC",
      "uriSpec" : {
        "parts" : [ {
          "value" : "http:",
          "variable" : false
        }, {
          "value" : ":",
          "variable" : false
        } ]
      }
    }



## [Helix Service Registry](http://helix.apache.org/0.7.0-incubating-docs/recipes/service_discovery.html)

This is inside Helix somewhere, used in LI in production at scale -and worth looking at. LI separate their Helix Zookeeper Quorum from their application-layer quorum, to isolate load.

Notable features

1. The registry is also the liveness view of the deployed application. Client's aren't watching the service registry for changes, they are watching Helix's model of the deployed application.
1. The deployed application can pick up changes to its state the same way, allowing for live application manipulation.
1. Tracks nodes that continually join/leave the group and drops them as unreliable.

## Twill Service Registry

Twill's [service registry code](http://twill.incubator.apache.org/apidocs/index.html), lets applications register a  [(hostname, port)](http://twill.incubator.apache.org/apidocs/org/apache/twill/discovery/Discoverable.html) pair in the registry by a name, a name by which clients can look up and enumerate all services with a specific name.

Clients can subscribe to changes in the list of services with a specific name -so picking up the arrival and departure of instances, and probe to see if a previously discovered entity is still registered.

Zookeeper- and in-memory registry implementations are provided.

One nice feature about this architecture -and Twill in general- is that its general single-method callback model means that it segues nicely into Java-8 lambda-expressions. This is something to retain in a YARN-wide service registry.

Comparing it to curator, it offers a proper subset of curator's registered services [ServiceInstance](http://curator.apache.org/apidocs/org/apache/curator/x/discovery/ServiceInstance.html) -implying that you could publish and retrieve Curator-registered services via a new implementation of Twill's DiscoveryService. This would require extensions to the curator service discovery client allow ZK nodes to be watched for changes. This is a feature that would be useful in many use cases -such as watching service availability across a cluster, or simply blocking until a dependent service was launched.

As with curator, the amount of information that can be published isn't enough for management tools to make effective use of the service registration, while for slider there's no way to publish configuration data. However a YARN registry will inevitably be a superset of the Twill client's enumerated and retrieved data -so if its registration API were sufficient to register a minimal service, supporting the YARN registry via Twill's existing API should be straightforward.

## Twitter Commons Service Registration

[Twitter Commons](https://github.com/twitter/commons) has a service registration library, which allows for registration of sets of servers, [publishing the hostname and port of each](http://twitter.github.io/commons/apidocs/com/twitter/common/service/registration/package-tree.html)., along with a map of string properties.

Zookeeper based, it suffices if all servers are identical and only publishing single (hostname, port) pairs for callers.

## AirBnB Smartstack

SmartStack is [Air BnB's cloud-based service discovery system](http://nerds.airbnb.com/smartstack-service-discovery-cloud/).

It has two parts, *Nerve* and *Synapse*:

[**Nerve**](https://github.com/airbnb/nerve) is a ruby agent designed to monitor processes and register healthy instances in ZK (or to a mock reporter). It includes [probes for TCP ports, HTTP and rabbitMQ](https://github.com/airbnb/nerve/tree/master/lib/nerve/service_watcher). It's [a fairly simple liveness monitor](https://github.com/airbnb/nerve/blob/master/lib/nerve/service_watcher.rb).

[**Synapse**](https://github.com/airbnb/synapse) takes the data and uses it to configure [HAProxy instances](http://haproxy.1wt.eu/). HAProxy handles the load balancing, queuing and integrating liveness probes into the queues. Synapse generates all the configuration files for an instance -but also tries to reconfigure the live instances via their socket APIs, 

Alongside these, AirBnB have another published project on Github, [Optica](https://github.com/airbnb/optica), which is a web application for nodes to register themselves with (POST) and for others to query. It publishes events to RabbitMQ, and again uses ZK to store state.

AirBnB do complain a bit about ZK and its brittleness. They do mention that they suspect it is due to bugs in the Ruby ZK client library. This may be exacerbated by in-cloud deployments. Hard-coding the list of ZK nodes may work for a physical cluster, but in a virtualized cluster, the hostnames/IP Addresses of those nodes may change -leading to a meta-discovery problem: how to find the ZK quorum -especially if you can't control the DNS servers.

## [Apache Directory](http://directory.apache.org/apacheds/)

This is an embeddable LDAP server

* Embeddable inside Java apps

* Supports Kerberos alongside X.500 auth. It can actually act as a Key server and TGT if desired.

* Supports DNS and DHCP queries.

* Accessible via classic LDAP APIs.

This isn't a registry service directly, though LDAP queries do make enumeration of services *and configuration data* straightforward. As LDAP libraries are common across languages -even built in to the Java runtime- LDAP support makes publishing information to arbitrary clients relatively straightforward.

If service information were to be published via LDAP, then it should allow IT-managed LDAP services to both host this information, and publish configuration data. This would be relevant for classic Hadoop applications if we were to move the Configuration class to support back-end configuration sources beyond XML files on the classpath.

# Proposal
