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

# Apache Slider Resource Specification
Resource specification is an input to Slider to specify the Yarn resource needs for each component type that belong to the application.

An example resource requirement for an application that has two components "master" and "worker" is as follows. Slider will automatically add the requirements for the AppMaster for the application. This compoent is named "slider-appmaster".

Some parameters that can be specified for a component instance include:

* `yarn.memory`: amount of memory requried for the component instance
* `yarn.vcores`: number of vcores requested
* `yarn.role.priority`: each component must be assigned unique priority. Component with higher priority come up earlier than components with lower priority
* `yarn.component.instances`: number of instances for this component type

Sample:

    {
      "schema" : "http://example.org/specification/v2.0.0",
      "metadata" : {
      },
      "global" : {
      },
      "components" : {
        "HBASE_MASTER" : {
          "yarn.role.priority" : "1",
          "yarn.component.instances" : "1"
          "yarn.memory" : "768",
          "yarn.vcores" : "1"
        },
        "slider-appmaster" : {
        },
        "HBASE_REGIONSERVER" : {
          "yarn.role.priority" : "2",
          "yarn.component.instances" : "1"
        }
      }
    }

