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

# Core Configuration Specification


## Terminology


*Application* A single application, such as an HBase cluster. An application
is distribed across the YARN cluster.

*Component* A single executable part of the larger application. An application
may have multiple components, and multiple instances of each component. 

*YARN* Yet Another Resource Negotiator

*YARN Resource Requirements* The requirements for a YARN resource request.
Currently this consists of RAM and CPU requirements.

*YARN Container*. An allocation portion of a servers resources granted
to satisfy the requested YARN resource requirements. A process can be deployed
to a container.


*`resources.json`*: A file that describes the
size of the application in terms of its component requirements: how many,
and what their resource requirements are. 

*`application.json`*: A file that describes the
size of the application in terms of its component requirements: how many,
and what their resource requirements are. 

## Structure

Configurations are stored in well-formed JSON files. 
1. Text MUST be saved in the UTF-8 format.
1. Duplicate entries MUST NOT occur in any section.
1. The ordering of elements is NOT significant.

The JSON specification files all have a similar structure

1. A `schema` string indicating version. Currently this is temporarily set to

        "http://example.org/specification/v2.0.0"
   
        
1. A global section, `/global` containing string properties
1. A component  section, `/components`.
1. 0 or more sections under `/components` for each component, identified by component name,
 containing string properties.
1. 0 or 1 section `/metadata` containing arbitrary metadata (such as a description,
author, or any other information that is not parsed or processed directly).


The simplest valid specification file is 
    
    {
      "schema": "http://example.org/specification/v2.0.0",

      "global": {
      },
      "components": {
      }
    }


## Property inheritance model and *resolution*


There is a simple global to component inheritance model.

1. Properties defined in `/global` define parameters across the entire application.
1. Properties defined a section under `/components` define parameters for
a specific component in the application.
1. All global properties are propagated to each component.
1. A component section may override any global property.
1. The final set of configuration properties for a component is the global
properties extended and overridden by the global set.
1. The process of expanding the properties is termed *resolution*; the *resolved*
specification is the outcome.
1. There is NO form of explicitly cross-referencing another attribute. This
MAY be added in future.
1. There is NO sharing of information from the different `.json` files in a
an application configuration.

### Example

Here is an example configuration

    {
      "schema": "http://example.org/specification/v2.0.0",

      "global": {
        "g1": "a",
        "g2": "b"
      },
      "components": {
        "simple": {
        },
        "master": {
          "name": "m",
          "g1": "overridden"
    
        },
        "worker": {
          "name": "w",
          "g1": "overridden-by-worker",
          "timeout": "1000"
        }
      }
    }
    
The `/global` section defines two properties

    g1="a"
    g2="b"
 
These are the values visible to any part of the application which is
not itself one of the components. 


There are three components defined, `simple`, `master` and `worker`.
 

#### component `simple`:
 
    g1="a"
    g2="b"


No settings have been defined specifically for the component; the global
settings are applied.

#### component `master`:
 
    name="m",
    g1="overridden"
    g2="b"

A new attribute, `name`, has been defined with the value `"m"`, and the 
global property `g1` has been overridden with the new value, `"overridden"`.
The global property `g2` is passed down unchanged.


#### component `worker`:
 
    name="w",
    g1="overridden-by-worker"
    g2="b"
    timeout: "1000"
    
A new attribute, `name`, has been defined with the value `"w"`, and another,
`timeout`, value "1000". 

The global property `g1` has been overridden with the new value, `"overridden-by-worker"`.

The global property `g2` is passed down unchanged.

This example shows some key points about the design

* each component gets its own map of properties, which is independent from
  that of other components.
* all global properties are either present or overridden by a new value.
  They can not be "undefined"
* new properties defined in a component are not visible to any other component.
 
The final *resolved* model is as follows
    
    {
      "schema": "http://example.org/specification/v2.0.0",

      "global": {
        "g1": "a",
        "g2": "b"
      },
      "components": {
        "simple": {
          "g1": "a",
          "g2": "b"
        },
        "master": {
          "name": "m",
          "g1": "overridden",
          "g2": "b"
        },
        "worker": {
          "name": "m",
          "g1": "overridden-by-worker",
          "g2": "b",
          "timeout": "1000"
        }
      }
    }

This the specification JSON that would have generate exactly the same result as
in the example, without any propagation of data from the global section
to individual components. 

Note that a resolved specification can still have the resolution operation applied
to it -it just does not have any effect.
 
## Metadata

The metadata section can contain arbitrary string values for use in diagnostics
and by other applications.

To avoid conflict with other applications, please use a unique name in strings,
such as java-style package names.
  
# Resource Requirements: `resources.json`

This file declares the resource requirements for YARN for the components
of an application.

`instances`: the number of instances of a role desired.
`yarn.vcores`: number of "virtual"  required by a component.
`yarn.memory`: the number of megabytes required by a component.

  
    {
      "schema": "http://example.org/specification/v2.0.0",

      "metadata": {
        "description": "example of a resources file"
      },
      
      "global": {
        "yarn.vcores": "1",
        "yarn.memory": "512"
      },
      
      "components": {
        "master": {
          "instances": "1",
          "yarn.memory": "1024"
        },
        "worker": {
          "instances":"5"
        }
      }
    }

The resolved file would be
  
    {
      "schema": "http://example.org/specification/v2.0.0",

      "metadata": {
        "description": "example of a resources file"
      },
      
      "global": {
        "yarn.vcores": "1",
        "yarn.memory": "512"
      },
      
      "components": {
        "master": {
          "instances": "1",
          "yarn.vcores": "1",
          "yarn.memory": "1024"
        },
        "worker": {
          "instances":"5",
          "yarn.vcores": "1",
          "yarn.memory": "512"
        }
      }
    }

This declares this deployment of the application to consist of one instance of
the master component, using 1 vcore and 1024MB of RAM, and five worker components
each using one vcore and 512 MB of RAM.


## Internal information, `internal.json`
 
This contains internal data related to the deployment -it is not
intended for manual editing.

There MAY be a component, `diagnostics`. If defined, its content contains
diagnostic information for support calls, and MUST NOT be interpreted
during application deployment, (though it may be included in the generation
of diagnostics reports)


    {
      "schema": "http://example.org/specification/v2.0.0",

      "metadata": {
        "description": "Internal configuration DO NOT EDIT"
      },
      "global": {
        "name": "small_cluster",
        "application": "hdfs://cluster:8020/apps/hbase/v/1.0.0/application.tar"
      },
      "components": {
    
        "diagnostics": {
          "create.hadoop.deployed.info": "(release-2.3.0) @dfe463",
          "create.hadoop.build.info": "2.3.0",
          "create.time.millis": "1393512091276",
          "create.time": "27 Feb 2014 14:41:31 GMT"
        }
      }
    }


## Deployment specification: `app_configuration.json`


This defines parameters that are to be used when creating the instance of the
application, and instances of the individual components.
    
    {
      "schema": "http://example.org/specification/v2.0.0",

      "global": {
    
        "zookeeper.port": "2181",
        "zookeeper.path": "/yarnapps_small_cluster",
        "zookeeper.hosts": "zoo1,zoo2,zoo3",
        "env.MALLOC_ARENA_MAX": "4",
        "site.hbase.master.startup.retainassign": "true",
        "site.fs.defaultFS": "hdfs://cluster:8020",
        "site.fs.default.name": "hdfs://cluster:8020",
        "site.hbase.master.info.port": "0",
        "site.hbase.regionserver.info.port": "0"
      },
      "components": {
    
        "worker": {
          "jvm.heapsize": "512M"
        },
        "master": {
          "jvm.heapsize": "512M"
        }
      }
    }
      
The resolved specification defines the values that are passed to the
different components.

    {
      "schema": "http://example.org/specification/v2.0.0",

      "global": {
        "zookeeper.port": "2181",
        "zookeeper.path": "/yarnapps_small_cluster",
        "zookeeper.hosts": "zoo1,zoo2,zoo3",
        "env.MALLOC_ARENA_MAX": "4",
        "site.hbase.master.startup.retainassign": "true",
        "site.fs.defaultFS": "hdfs://cluster:8020",
        "site.fs.default.name": "hdfs://cluster:8020",
        "site.hbase.master.info.port": "0",
        "site.hbase.regionserver.info.port": "0"
      },
      "components": {
    
        "worker": {
          "zookeeper.port": "2181",
          "zookeeper.path": "/yarnapps_small_cluster",
          "zookeeper.hosts": "zoo1,zoo2,zoo3",
          "env.MALLOC_ARENA_MAX": "4",
          "site.hbase.master.startup.retainassign": "true",
          "site.fs.defaultFS": "hdfs://cluster:8020",
          "site.fs.default.name": "hdfs://cluster:8020",
          "site.hbase.master.info.port": "0",
          "site.hbase.regionserver.info.port": "0",
          "jvm.heapsize": "512M"
        },
        "master": {
          "zookeeper.port": "2181",
          "zookeeper.path": "/yarnapps_small_cluster",
          "zookeeper.hosts": "zoo1,zoo2,zoo3",
          "env.MALLOC_ARENA_MAX": "4",
          "site.hbase.master.startup.retainassign": "true",
          "site.fs.defaultFS": "hdfs://cluster:8020",
          "site.fs.default.name": "hdfs://cluster:8020",
          "site.hbase.master.info.port": "0",
          "site.hbase.regionserver.info.port": "0",
          "jvm.heapsize": "512M"
        }
      }
    }
    
The `site.` properties have been passed down to each component, components
whose templates may generate local site configurations. The override model
does not prevent any component from overriding global configuration so as
to create local configurations incompatible with the global state. (i.e.,
there is no way to declare an attribute as final). It is the responsibility
of the author of the configuration file (and their tools) to detect such issues.
