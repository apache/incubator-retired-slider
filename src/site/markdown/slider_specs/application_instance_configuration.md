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

#App Instance Configuration

App Instance Configuration is the configuration override provided by the application owner when creating an application instance using Slider. This configuration values override the default configuration available in the App Package.

Instance configuration is a JSON formatted doc in the following form:

```
{
    "configurations": {
        "app-global-config": {
        },
        "config-type-1": {
        },
        "config-type-2": {
        },
    }
}
```


The configuration overrides are organized in a two level structure where name-value pairs are grouped on the basis of config types they belong to. App instantiator can provide arbitrary custom name-value pairs within a config type defined in the AppPackage or can create a completely new config type that does not exist in the AppAPackage. The interpretation of the configuration is entirely up to the command implementations present in the AppPackage. Slider will simply merge the configs with the InstanceConfiguration being higher priority than that default configuration and hand it off to the app commands.

A sample config for hbase may be as follows:


```
{
    "configurations": {
        "hbase-log4j": {
            "log4j.logger.org.apache.zookeeper": "INFO",
            "log4j.logger.org.apache.hadoop.hbase": "DEBUG"
        },
        "hbase-site": {
            "hbase.hstore.flush.retries.number": "120",
            "hbase.regionserver.info.port": "",
            "hbase.master.info.port": "60010"
        }
}
```


The above config overwrites few parameters in hbase-site and hbase-log4j files. Several config properties such as "hbase.zookeeper.quorum" for hbase may not be known to the user at the time of app instantiation. These configurations will be provided by the Slider infrastructure in a well-known form so that the app implementation can read and set them while instantiating component instances..

