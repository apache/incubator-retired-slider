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

# Apache Slider: Service Registry End-to-End Scenarios

## AM startup

1. AM starts, reads in configuration, creates provider

2. AM builds web site, involving provider in process  (*there's a possible race condition here, due to the AM registration sequence)*

3. AM registers self with RM, including web and IPC ports, and receives list of existing containers; container loss notifications come in asynchronously *(which is why the AM startup process is in a synchronized block)*

4. AM inits it's `ApplicationState` instance with the config, instance description and RM-supplied container list.

5. AM creates service registry client using ZK quorum and path provided when AM was started

6. AM registers standard endpoints: RPC, WebUI, REST APIs

7. AM registers standard content it can serve (e.g `yarn-site.xml`)

8. AM passes registry to provider in `bind()` operation.

9. AM triggers review of application state, requesting/releasing nodes as appropriate

## Agent Startup: standalone

1. Container is issued to AM

2. AM chooses component, launches agent on it -with URL of AM a parameter (TODO: Add registry bonding of ZK quorum and path)

3. Agent starts up.

4. Agent locates AM via URL/ZK info

5. Agent heartbeats in with state

6. AM gives agent next state command.

## AM gets state from agent:

1. Agent heartbeats in

2. AM decides if it wants to receive config 

3. AM issues request for state information -all (dynamic) config data

4. Agent receives it

5. Agent returns all config state, including: hostnames, allocated ports, generated values (e.g. database connection strings, URLs) - as two-level (allows agent to define which config options are relevant to which document)

## AM saves state for serving

1. AM saves state in RAM (assumptions: small, will rebuild on restart)

2. AM updates service registry with list of content that can be served up and URLs to retrieve them.

3. AM fields HTTP GET requests on content

## AM Serves content

A simple REST service serves up content on paths published to the service registry. It is also possible to enumerate documents published by GET  operations on parent paths.

1. On GET command, AM locates referenced agent values

2. AM builds up response document from K-V pairs. This can be in a limited set of formats: Hadoop XML, Java properties, YAML, CSV, HTTP, JSON chosen as ? type param. (this generation is done from template processing in AM using slider.core.template module)

3. response is streamed with headers of : `content-type`, `content-length`, do not cache in proxy, expires,* (with expiry date chosen as ??)*

# Slider Client

Currently slider client enumerates the YARN registry looking for slider instances -including any instances of the same application running before launching a cluster. 

This 

* has race conditions
* has scale limitations `O(apps-in-YARN-cluster)` + `O(completed-apps-in-RM-memory)`
* only retrieves configuration information from slider-deployed application instances. *We do not need to restrict ourselves here.*

## Slider Client lists applications

    slider registry --list [--servicetype <application-type>]

1. Client starts

2. Client creates creates service registry client using ZK quorum and path provided in client config properties (slider-client.xml)

3. Client enumerates registered services and lists them

## Slider Client lists content published by an application instance

    slider registry <instance> --listconf  [--servicetype <application-type>]

1. Client starts

2. Client creates creates service registry client using ZK quorum and path provided in client config properties (slider-client.xml)

3. Client locates registered service entry -or fails

4. Client retrieves service data, specifically the listing of published documents

5. Client displays list of content

## Slider Client retrieves content published by an application instance

    slider registry <instance> --getconf <document> [--format (xml|properties|text|html|csv|yaml|json,...) [--dest <file>]  [--servicetype <application-type>]

1. Client starts

2. Client creates creates service registry client using ZK quorum and path provided in client config properties (slider-client.xml)

3. Client locates registered service entry -or fails

4. Client retrieves service data, specifically the listing of published documents

5. Client locates URL of content

6. Client builds GET request including format

7. Client executes command, follows redirects, validates content length against supplied data.

8. Client prints response to console or saves to output file. This is the path specified as a destination, or, if that path refers to a directory, to
a file underneath.

## Slider Client retrieves content set published by an application instance

Here a set of documents published is retrieved in the desired format of an application.

## Slider Client retrieves document and applies template to it

Here a set of documents published is retrieved in the desired format of an application.

    slider registry <instance> --source <document> [--template <path-to-template>] [--outfile <file>]  [--servicetype <application-type>]

1. document is retrieved as before, using a simple format such as json to retrieve it.

2. The document is parsed and converted back into K-V pairs

3. A template using a common/defined template library is applied to the content , generating the final output.

Template paths may include local filesystem paths or (somehow) something in a package file

