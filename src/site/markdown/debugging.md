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

# Debugging Slider
There are a number of options available to you for debugging Slider applications.  They include:

* Using Slider logging
* IDE-based remote debugging of the Application Master

## Using Slider logging
There are a number of options for viewing the generated log files:

1. Using a web browser
2. Accessing the host machine
  
### Using a web browser

The log files are accessible via the Yarn Resource Manager UI.  From the main page (e.g. http://YARN_RESOURCE_MGR_HOST:8088), click on the link for the application instance of interest, and then click on the "logs" link.  This will present you with a page with links to the slider-err.txt file and the slider-out.txt file.  The former is the file you should select.  Once the log page is presented, click on the link at the top of the page ("Click here for full log") to view the entire file.

### Accessing the host machine

If access to other log files is required, there is the option of logging in to the host machine on which the application component is running.  The root directory for all Yarn associated files is the value of "yarn.nodemanager.log-dirs" in yarn-site.xml - e.g. /hadoop/yarn/log.  Below the root directory you will find an application and container sub-directory (e.g. /application_1398372047522_0009/container_1398372047522_0009_01_000001/).  Below the container directory you will find any log files associated with the processes running in the given Yarn container.

Within a container log the following files are useful while debugging the application.

**agent.log** 
  
E.g. application_1398098639743_0024/container_1398098639743_0024_01_000003/infra/log/agent.log
This file contains the logs from the Slider-Agent.

**application component log**

E.g. ./log/application_1398098639743_0024/container_1398098639743_0024_01_000003/app/log/hbase-yarn-regionserver-c6403.ambari.apache.org.log

The location of the application log is defined by the application. "${AGENT_LOG_ROOT}" is a symbol available to the app developers to use as a root folder for logging.

**agent operations log**

E.g. ./log/application_1398098639743_0024/container_1398098639743_0024_01_000003/app/command-log/

The command logs produced by the slider-agent are available in the "command-log" folder relative to "${AGENT_LOG_ROOT}"/app


## IDE-based remote debugging of the Application Master

For situtations in which the logging does not yield enough information to debug an issue, the user has the option of specifying JVM command line options for the Application Master that enable attaching to the running process with a debugger (e.g. the remote debugging facilities in Eclipse or Intellij IDEA).  In order to specify the JVM options, edit the application configuration file (the file specified as the --template argument value on the command line for cluster creation) and specify the "jvm.opts" property for the "slider-appmaster" component:

	`"components": {
    	"slider-appmaster": {
      		"jvm.heapsize": "256M",
      		"jvm.opts": "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
    	},
 		...`
 		
You may specify "suspend=y" in the line above if you wish to have the application master process wait for the debugger to attach before beginning its processing.
