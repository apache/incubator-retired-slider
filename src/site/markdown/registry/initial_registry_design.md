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

# April 2014 Initial Registry Design for Apache Slider

This is the plan for the initial registry design.

1. Use Apache Curator [service discovery code](http://curator.apache.org/curator-x-discovery/index.html). 

2. AMs to register as (user, name). Maybe "service type" if we add that as an option in the slider configs

3. Lift "external view" term from Helix -concept that this is the public view, not internal.

4. application/properties section to list app-wide values

5. application/services section to list public service URLs; publish each as unique-ID-> (human name, URL, human text). code can resolve from UniqueID; UIs can use human data.

6. String Template 2 templates for generation of output (rationale:  library for Python Java, .NET)

7. Java CLI to retrieve values from ZK and apply named template (local, hdfs). Include ability to restrict to list of named properties (pattern match).

8. AM to serve up curator service (later -host in RM? elsewhere?)

### forwards-compatilibity

1. This initial design will hide the fact that Apache Curator is being used to discover services,
by storing information in the payload, `ServiceInstanceData` rather than in (the minimdal) curator
service entries themselves. If we move to an alternate registry, provided we
can use the same datatype -or map to it- changes should not be visible.

1. The first implementation will not support watching for changes.

### Initial templates 

* hadoop XML conf files

* Java properties file

* HTML listing of services



## Example Curator Service Entry

This is the prototype's content

Toplevel

    service CuratorServiceInstance{name='slider', id='stevel.test_registry_am', address='192.168.1.101', port=62552, sslPort=null, payload=org.apache.slider.core.registry.info.ServiceInstanceData@4e9af21b, registrationTimeUTC=1397574073203, serviceType=DYNAMIC, uriSpec=org.apache.curator.x.discovery.UriSpec@ef8dacf0} 

Slider payload.

    payload=
    {
      "internalView" : {
        "endpoints" : {
          "/agents" : {
            "value" : "http://stevel-8.local:62552/ws/v1/slider/agents",
            "protocol" : "http",
            "type" : "url",
            "description" : "Agent API"
          }
        },
        "settings" : { }
      },
    
      "externalView" : {
        "endpoints" : {
          "/mgmt" : {
            "value" : "http://stevel-8.local:62552/ws/v1/slider/mgmt",
            "protocol" : "http",
            "type" : "url",
            "description" : "Management API"
          },
    
          "slider/IPC" : {
            "value" : "stevel-8.local/192.168.1.101:62550",
            "protocol" : "org.apache.hadoop.ipc.Protobuf",
            "type" : "address",
            "description" : "Slider AM RPC"
          },
          "registry" : {
            "value" : "http://stevel-8.local:62552/ws/registry",
            "protocol" : "http",
            "type" : "url",
            "description" : "Registry"
          }
        },
        "settings" : { }
      }
    }

 

   

