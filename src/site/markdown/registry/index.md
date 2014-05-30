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
  
# Apache Slider Service Registry

The service registry is a core part of the Slider Architecture -it is how
dynamically generated configurations are published for clients to pick up.

The need for a service registry goes beyond Slider, however. We effectively
have application-specific registries for HBase and Accumulo, and explicit
registries in Apache Helix and Apache Twill, as well as re-usable registry
code in Apache Curator.

[YARN-913](https://issues.apache.org/jira/browse/YARN-913) covers the need
for YARN itself to have a service registry. This would be the ideal ultimate
solution -it would operate at a fixed location/ZK path, and would be guaranteed
to be on all YARN clusters, so code could be written expecting it to be there.

It could also be used to publish binding data from static applications,
including HBase, Accumulo, Oozie, -applications deployed by management tools.
Unless/until these applications self-published their binding data, it would
be the duty of the management tools to do the registration.



## Contents

1. [YARN Application Registration and Binding: the Problem](the_YARN_application_registration_and_binding_problem.html)
1. [A YARN Service Registry](a_YARN_service_registry.html)
1. [April 2014 Initial Registry Design](initial_registry_design.html)
1. [Service Registry End-to-End Scenarios](service_registry_end_to_end_scenario.html)
1. [P2P Service Registries](p2p_service_registries.html)
1. [References](references.html)
