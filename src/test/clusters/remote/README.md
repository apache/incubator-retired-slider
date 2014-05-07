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
  
 # README
 
This config is required for tests defined in **org.apache.slider.funtest.lifecycle**.

**Test setup**

Edit config file src/test/clusters/remote/slider/slider-client.xml and ensure that the host names are accurate for the test cluster.

**User setup**

Ensure that the user, running the test, is present on the cluster against which you are running the tests. The user must be a member of the hadoop group.

E.g. adduser **testuser** -d /home/**testuser** -G hadoop -m

**HDFS Setup**

Set up various test folders and load the agent and app packages.

Set up hdfs folders for slider and test user

*  su hdfs
*  hdfs dfs -mkdir /slider
*  hdfs dfs -chown testuser:hdfs /slider
*  hdfs dfs -mkdir /user/testuser
*  hdfs dfs -chown testuser:hdfs /user/testuser

Set up agent package and config

*  su **testuser**
*  hdfs dfs -mkdir /slider/agent
*  hdfs dfs -mkdir /slider/agent/conf
*  hdfs dfs -copyFromLocal <share>/slider-agent.tar.gz /slider/agent
*  hdfs dfs -copyFromLocal <share>/agent.ini /slider/agent/conf

Add app packages 

*  hdfs dfs -copyFromLocal slider-core/src/test/app_packages/test_command_log/cmd_log_app_pkg.tar

**Enable/Execute the tests**

To enable the test ensure that *slider.test.agent.enabled* is set to *true*. The tests can be executed through the following mvn command executed at slider/slider-funtest.

```
mvn test -Dslider.conf.dir=../src/test/clusters/remote/slider -Dtest=TestAppsThroughAgent -DfailIfNoTests=false
```
 