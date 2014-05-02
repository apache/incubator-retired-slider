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

#App Package

All application artifacts, app definition, app configuration, scripts are packaged into a structured single package that can be handed off to any YARN application deployment tool including Slider

## Overall Structure

App package is a tarball containing all application artifacts. App package contains the following items:

* **app definition file**
application structure, content, definition, supported platforms, version, etc.

* **default configurations folder**
various configurations and configuration files associated with the application

* **cmd_impl folder**
management operations for the application/component

 * **scripts folder**
various scripts that implement management operations

 * **templates folder**
various templates used by the application

 * **files folder**
other scripts, txt files, tarballs, etc.


![Image](../images/app_package_sample_04.png?raw=true)

The example above shows a semi-expanded view of an application "HBASE-YARN-APP" and the package structure for OOZIE command scripts.

## app definition

App definition is a file named "metainfo.xml". The file contains application definition as described in [Application Definition](application_definition.md). 

## default configurations

This folder consists of various config files containing default configuration as described in [App Configuration](application_configuration.md).

## package folder

package includes the "implementation" of all management operations. The folders within are divided into scripts, templates, and files.

### scripts folder

Scripts are the implementation of management operations. There are five default operations and a composite operation. "restart" can be redefined to have a custom implementation.

1. install

2. configure

3. start

4. stop

5. status

6. restart (by default calls stop + start)

The script specified in the metainfo is expected to understand the command. It can choose to call other scripts based on how the application author organizes the code base. For example:

```
class OozieServer(Script):
  def install(self, env):
    self.install_packages(env)
    
  def configure(self, env):
    import params
    env.set_params(params)
    oozie(is_server=True)
    
  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env)
    oozie_service(action='start')
    
  def stop(self, env):
    import params
    env.set_params(params)
    oozie_service(action='stop')

  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.pid_file)
```


The scripts are invoked in the following manner:

`python SCRIPT COMMAND JSON_FILE PACKAGE_ROOT STRUCTURED_OUT_FILE`

* SCRIPT is the top level script that implements the commands for the component. 

* COMMAND is one of the six commands listed above or can be a custom command as defined in Application Definition

* JSON_FILE includes all configuration parameters and the values

* PACKAGE_ROOT is the root folder of the package. From this folder, its possible to access files, scripts, templates, packages (e.g. tarballs), etc. The App author has complete control over the structure of the package

* STRUCTURED_OUT_FILE is the file where the script can output structured data. The management infrastructure is expected to automatically reports back STD_OUT and STD_ERR.

A separate document (link TBD) discusses how the scripts are developed and the structure of the JSON_FILE containing the parameters.

### templates folder

templates are configurable text files that are NOT regular config files. *A library has been developed that can materialize a complete site configuration file from a property bag and therefore are not generated from templates.* Other files such as env sh files, log4j properties file, etc. may be derived from a template. Again, the implementor can choose to create these files from scratch and not use templates. The following operations are allowed during template expansion:

* variable expansion

* if condition

* for loop

* ...

Sample template file for dfs.exclude file to list excluded/decommissioned hosts. hdfs_exclude_files in the property defined in params.py which is populated from config parameters defined in JSON_FILE.

```
{% if hdfs_exclude_file %} 
{% for host in hdfs_exclude_file %}
{{host}}
{% endfor %}
{% endif %}
```


### files folder

files is a directory to store any other files that are needed for management operations. Sample files stored here are tarballs used to install the application, shell scripts used by various operations.

