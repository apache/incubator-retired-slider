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
  
# P2P Service Registries for Apache Slider

Alongside the centralized service registries, there's much prior work on
P2P discovery systems, especially for mobile and consumer devices.

They perform some multicast- or distributed hash table-based lookup,
and tend to have common limitations:

* scalability

* the bootstrapping problem

* security: can you trust the results to be honest?

* consistency: can you trust the results to be complete and current?

Bootstrapping is usually done via multicast, possibly then switching
to unicast for better scale. As multicasting doesn't work in cloud
infrastructures, none of the services work unmodified  in public
clouds. There's multiple anecdotes of
[Amazon's SimpleDB service](http://aws.amazon.com/simpledb/) being used as a
registry for in-EC2 applications. At the very least, this service and its
equivalents in other cloud providers could be used to bootstrap ZK client
bindings in cloud environments. 

## Service Location Protocol 

Service Location Protocol is a protocol for discovery services that came out
of Sun, Novell and others -it is still available for printer discovery and
suchlike

It supports both a multicast discovery mechanism, and a unicast protocol
to talk to a Directory Agent -an agent that is itself discovered by multicast
requests, or by listening for the agent's intermittent multicast announcements.

There's an extension to DHCP, RFC2610, which added the ability for DHCP to
advertise Directory Agents -this was designed to solve the bootstrap problem
(though not necessarily security or in-cloud deployment). Apart from a few
mentions in Windows Server technical notes, it does not appear to exist.

* [[RFC2608](http://www.ietf.org/rfc/rfc2608.txt)] *Service Location Protocol, Version 2* , IEEE, 1999

* [[RFC3224](http://www.ietf.org/rfc/rfc3224.txt)] *Vendor Extensions for Service Location Protocol, Version 2*, IETF, 2003

* [[RFC2610](http://www.ietf.org/rfc/rfc2610.txt)] *DHCP Options for Service Location Protocol, IETF, 1999*

## [Zeroconf](http://www.zeroconf.org/)

The multicast discovery service implemented in Apple's Bonjour system
--multicasting DNS lookups to all peers in the subnet.

This allows for URLs and hostnames to be dynamically positioned, with
DNS domain searches allowing for enumeration of service groups. 

This protocol scales very badly; the load on *every* client in the
subnet is is O(DNS-queries-across-subnet), hence implicitly `O(devices)*O(device-activity)`. 

The special domains `_tcp.`, `_udp.`  and their subdomains can also be
served up via a normal DNS server.

##  [Jini/Apache River](http://river.apache.org/doc/specs/html/lookup-spec.html)

Attribute-driven service enumeration, which drives the, Java-client-only
model of downloading client-side code. There's no requirement for the remote
services to be in Java, only that drivers are.

## [Serf](http://www.serfdom.io/)  

This is a library that implements the [SWIM protocol](http://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf) to propagate information around a cluster. Apparently works in virtualized clusters too. It's already been used in a Flume-on-Hoya provider.

## [Anubis](http://sourceforge.net/p/smartfrog/svn/HEAD/tree/trunk/core/components/anubis/)

An HP Labs-built [High Availability tuple-space](http://sourceforge.net/p/smartfrog/svn/HEAD/tree/trunk/core/components/anubis/doc/HPL-2005-72.pdf?format=raw) in SmartFrog; used in production in some of HP's telco products. An agent publishes facts into the T-Space, and within one heartbeat all other agents have it. One heart-beat later, unless there's been a change in the membership, the publisher knows the others have it. One heartbeat later the agents know the publisher knows it, etc.

Strengths: 

* The shared knowledge mechanism permits reasoning and mathematical proofs.

* Strict ordering between heartbeats implies an ordering in receipt.
This is stronger than ZK's guarantees.

* Lets you share a moderate amount of data (the longer the heartbeat
interval, the more data you can publish).

* Provided the JVM hosting the Anubis agent is also hosting the service,
liveness is implicit

* Secure to the extent that it can be locked down to allow only nodes with
mutual trust of HTTPS certificates to join the tuple-space.

Weaknesses

* (Currently) bootstraps via multicast discovery.

* Brittle to timing, especially on virtualized clusters where clocks are unpredictable.

It proved good for workload sharing -tasks can be published to it, any
agent can say "I'm working on it" and take up the work. If the process
fails, the task becomes available again. We used this for distributed scheduling in a rendering farm.

## [Carmen](http://www.hpl.hp.com/techreports/2002/HPL-2002-257)

This was another HP Labs project, related to the Cooltown "ubiquitous
computing" work, which was a decade too early to be relevant. It was
also positioned by management as a B2B platform, so ended up competing
with - and losing against - WS-* and UDDI. 

Carmen aimed to provide service discovery with both fixed services, and
with highly mobile client services that will roam around the network -they
are assumed to be wireless devices.

Services were published with and searched for by attributed, locality
was considered to be a key attribute -local instances of a service
prioritized. Those services with a static location and low rate of
change became the stable caches of service information -becoming,
as with skype, "supernodes". 

Bootstrapping the cluster relied on multicast, though alternatives
based on DHCP and DNS were proposed.

