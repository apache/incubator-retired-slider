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

#Resource Specification
Resource specification is an input to Slider to specify the Yarn resource needs for each component type that belong to the application.

Some parameters that can be specified for a component instance include:

* yarn.memory: amount of memory requried for the component instance
* env.MALLOC_ARENA_MAX: maximum number of memory pools used, arbitrary environment settings can be provided through format env.NAME_OF_THE_VARIABLE
* component.instances: number of instances requested
* component.name: name of the component 
* yarn.vcores: number of vcores requested

An example resource requirement for an application that has two components "master" and "worker" is as follows. Slider will automatically add the requirements for the AppMaster for the application. This compoent is named "slider".

```
"components" : {
    "worker" : {
      "yarn.memory" : "768",
      "env.MALLOC_ARENA_MAX" : "4",
      "component.instances" : "1",
      "component.name" : "worker",
      "yarn.vcores" : "1"
    },
    "slider" : {
      "yarn.memory" : "256",
      "env.MALLOC_ARENA_MAX" : "4",
      "component.instances" : "1",
      "component.name" : "slider",
      "yarn.vcores" : "1"
    },
    "master" : {
      "yarn.memory" : "1024",
      "env.MALLOC_ARENA_MAX" : "4",
      "component.instances" : "1",
      "component.name" : "master",
      "yarn.vcores" : "1"
    }
  }
```
