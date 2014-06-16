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

# Apache Slider YARN Application Registration and Binding: the Problem

## March 2014

# How to bind client applications to services dynamically placed applications?


There are some constraints here

1. The clients may be running outside the cluster -potentially over long-haul links.

1. The location of an application deployed in a YARN cluster cannot be predicted.

1. The ports used for application service endpoints cannot be hard-coded
or predicted. (Alternatively: if they are hard-coded, then Socket-In-Use exceptions may occur)

1: As components fail and get re-instantiated, their location may change. 
The rate of this depends on cluster and application stability; the longer
 lived the application, the more common it is.

Existing Hadoop client apps have a configuration problem of their own:
how are the settings in files such as `yarn-site.xml`picked up by today's
applications? This is an issue which has historically been out of scope
for Hadoop clusters -but if we are looking at registration and binding
of YARN applications, there should be no reason why
static applications cannot be discovered and bonded to using the same mechanisms. 

# Other constraints:

1. Reduce the amount of change needed in existing applications to a minimum 
---ideally none, though some pre-launch setup may be acceptable.

2. Prevent malicious applications from registering a service endpoints.

3. Scale with #of applications and #of clients; not overload on a cluster partitioning.

4. Offer a design that works with apps that are deployed in a YARN custer 
outside of Slider. Rationale: want a mechanism that works with pure-YARN apps

## Possible Solutions:

### ZK

Client applications use ZK to find services (addresses #1, #2 and #3).
Requires location code in the client.

HBase and Accumulo do this as part of a failover-ready design.

### DNS

Client apps use DNS to find services, with custom DNS server for a 
subdomain representing YARN services. Addresses #1; with a shortened TTL and 
no DNS address caching, #3. #2 addressed only if other DNS entries are used to
 publish service entries. 

Should support existing applications, with a configuration that is stable
over time. It does require the clients to not cache DNS addresses forever
(this must be explicitly set on Java applications,
irrespective of the published TTL). It generates a load on the DNS servers
that is `O(clients/TTL)`

Google Chubby offers a DNS service to handle this. ZK does not -yet.

### Floating IP Addresses

If the clients know/cache IP addresses of services, these addresses could be
floated across service instances. Linux HA has floating IP address support,
while Docker containers can make use of them, especially if an integrated DHCP
server handles the assignment of IP addresses to specific containers. 

ARP caching is the inevitable problem here, but it is still less brittle than
relying on applications to know not to cache IP addresses -and nor does it
place so much on DNS servers as short-TTL DNS entries.

### LDAP

Enterprise Directory services are used to publish/locate services. Requires
lookup into the directory on binding (#1, #2), re-lookup on failure (#3).
LDAP permissions can prevent untrusted applications registering.

* Works well with Windows registries.

* Less common Java-side, though possible -and implemented in the core Java
libraries. Spring-LDAP is focused on connection to an LDAP server
-not LDAP-driven application config.

### Registration Web Service

 Custom web service registration services used. 

* The sole WS-* one, UDDI, does not have a REST equivalent
--DNS is assumed to take on that role.

* Requires new client-side code anyway.

### Zookeeper URL Schema

Offer our own `zk://` URL; java & .NET implementations (others?) to resolve, browser plugins. 

* Would address requirements #1 & #3

* Cost: non-standard; needs an extension for every application/platform, and
will not work with tools such as CURL or web browsers

### AM-side config generation

App-side config generation-YARN applications to generate client-side
configuration files for launch-time information (#1, #2).
The AM can dynamically create these, and as the storage load is all in
the AM, does not consume as much resources in a central server as would 
publishing it all to that central server.

* Requires application to know of client-side applications to support -
and be able to generate to their configuration information (i.e. formatted files).

* Requires the AM to get all information from deployed application components
needed to generate bindings. Unless the AM can resolve YARN App templates,
need a way to get one of the components in the app to generate settings for
the entire cluster, and push them back.

* Needs to be repeated for all YARN apps, however deployed.

* Needs something similar for statically deployed applications.


### Client-side config generation

YARN app to publish attributes as key-val pairs, client-side code to read and
generate configs from  (#1, #2).  Example configuration generators could
create: Hadoop-client XML, Spring, tomcat, guice configs, something for .NET.

* Not limited to Hoya application deployments only.

* K-V pairs need to be published "somewhere". A structured section in the
ZK tree per app is the obvious location -though potentially expensive. An
alternative is AM-published data.

* Needs client-side code capable of extracting information from YARN cluster
to generate client-specific configuration.

* Assumes (key, value) pairs sufficient for client config generation. Again,
some template expansion will aid here (this time: client-side interpretation).

* Client config generators need to find and bind to the target application themselves.

 

Multiple options:

* Standard ZK structure for YARN applications (maybe: YARN itself to register
paths in ZK and set up child permissions,so enforcing security).

* Agents to push to ZK dynamic information as K-V pairs

* Agent provider on AM to fetch K-V pairs and include in status requests

* CLI to fetch app config keys, echo out responses (needs client log4j settings
to log all slf/log4j to stderr, so that app > results.txt would save results explicitly

*  client side code per app to generate specific binding information

### Load-balancer app Yarn App 

Spread requests round a set of registered handlers, e.g web servers. Support
plugins for session binding/sharding. 

Some web servers can do this already; a custom YARN app could use grizzy
embedded. Binding problem exists, but would support scaleable dispatch of values.

*  Could be offered as an AM extension (in provider, ...): scales well
with #of apps in cluster, but adds initial location/failover problems.

* If offered as a core-YARN service, location is handled via a fixed
URL. This could place high load on the service, even just 302 redirects.

