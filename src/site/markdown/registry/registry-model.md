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
  
# Registry

The service registry model is designed to support dynamically
deployed Slider applications, *and* statically deployed versions
of the same application -provided the latter also registers itself,
its public network services, and any configurations and files
that it wishes clients to be able to retrieve.

The architecture and implementation of this registry is not defined
here -instead the view of it seen by clients.

1. A 'service registry' exists in the YARN cluster into which
services can be registered. 

1. There is no restriction on the number of services that can be registered in
the registry, the type of service that may register, or even on how many
registered services an application running in the YARN cluster may register.

1. Services are registered by their type, owner and name. As an example,
Alice's slider-managed HBase cluster `ingress` would have a type `org.apache.hbase`,
owner `alice` and name `ingress`. 

1. In the case of Slider-managed services, there is a separate slider instance
registration which publishes information about slider itself. In the example
above, this would be (`org.apache.slider`,`alice`,`ingress`).

1. Services can publish information about themselves, with common entries being:

    * service name and description.
    * URLs of published web UIs and web service APIs
    * network address of other protocols
    
1. Services may also publish.    
    
    * URLs to configuration details
    * URLs documents published for use as client-side configuration data -either
      directly or through some form of processing.
    * public service-specific data, for use by applications that are aware of
      the specific service type.
    * internal service-specific data -for use by the components that comprise
      an application. This allows the registry to be used to glue together
      the application itself.
      
1. Services can be listed and examined.

1. Service-published configuration key-value pairs can be retrieved by clients

1. Service-published documents (and packages of such documents) can be
retrieved by clients.

1. There's no requirement for service instances to support any standard protocols;

1. Some protocols are defined which they MAY implement. For example, the protocol
to enumerate and retrieve configuration documents is designed to be implemented
by any service that wishes to publish such content.

1. In a secure cluster, the registry data may be restricted, along with any
service protocols offered by the registered services. 
