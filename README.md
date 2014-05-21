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

# Slider


Slider is a YARN application to deploy existing distributed applications on YARN, 
monitor them and make them larger or smaller as desired -even while 
the cluster is running.

Clusters can be stopped, "frozen" and restarted, "thawed" later; the distribution
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

## Open-Source Development

Slider is an open source project developed under the auspices of the [Apache Software Foundation](http://apache.org).

We welcome and encourage everyone using Slider to get involved!

### Mailing Lists

We have a single mailing list for developers and users of Slider: dev@slider.incubator.apache.org

1. You can subscribe to this by emailing dev-subscribe@slider.incubator.apache.org from the
email account to which you wish to subscribe from -and follow the instructions returned.
1. You can unsubscribe later by emailing dev-unsubscribe@slider.incubator.apache.org

There is a mailing list of every commit to the source code repository, commits@slider.incubator.apache.org.
This is generally only of interest to active developers.


### Bug reports

Bug reports and other issues can be filed on the [Apache Jira](https://issues.apache.org/jira/) server.
Please use the SLIDER project for filing the issues.

### Source code access

Read-only:

*  [https://git.apache.org/repos/asf/incubator-slider.git](https://git.apache.org/repos/asf/incubator-slider.git)
*  [https://github.com/apache/incubator-slider.git](https://github.com/apache/incubator-slider.git)

Read-write (for committers):

*  [https://git-wip-us.apache.org/repos/asf/incubator-slider.git](https://git-wip-us.apache.org/repos/asf/incubator-slider.git)
  

# License


  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   (http://www.apache.org/licenses/LICENSE-2.0)
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
