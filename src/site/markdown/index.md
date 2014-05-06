<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
  

# Slider: Dynamic YARN Applications



Slider is a YARN application to deploy existing distributed applications on YARN, 
monitor them and make them larger or smaller as desired -even while 
the application is running.

Applications can be stopped, "frozen" and restarted, "thawed" later; the distribution
of the deployed application across the YARN cluster is persisted -enabling
a best-effort placement close to the previous locations on a cluster thaw.
Applications which remember the previous placement of data (such as HBase)
can exhibit fast start-up times from this feature.

YARN itself monitors the health of 'YARN containers" hosting parts of 
the deployed application -it notifies the Slider manager application of container
failure. Slider then asks YARN for a new container, into which Slider deploys
a replacement for the failed component. As a result, Slider can keep the
size of managed applications consistent with the specified configuration, even
in the face of failures of servers in the cluster -as well as parts of the
application itself

Some of the features are:

* Allows users to create on-demand applications in a YARN cluster

* Allow different users/applications to run different versions of the application.

* Allow users to configure different application instances differently

* Stop / Suspend / Resume application instances as needed

* Expand / shrink application instances as needed

The Slider tool is a Java command line application.

The tool persists the information as JSON documents in HDFS.

Once the cluster has been started, the cluster can be made to grow or shrink
using the Slider commands. The cluster can also be stopped, *frozen*
and later resumed, *thawed*.
      
Slider implements all its functionality through YARN APIs and the existing
application shell scripts. The goal of the application was to have minimal
code changes and as of this writing, it has required few changes.

## Using 

* [Getting Started](getting_started.html)
* [Man Page](manpage.html)
* [Examples](examples.html)
* [Client Configuration](client-configuration.html)
* [Client Exit Codes](exitcodes.html)
* [Security](security.html)
* [Logging](logging.html)
* [How to define a new slider-packaged application](slider_specs/index.html)
* [Application configuration model](configuration/index.html)


## Developing 

* [Architecture](architecture/index.html)
* [Developing](developing/index.html)
* [Application Needs](app_needs.html)
* [Service Registry](registry/index.html)
