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

#Developing App Command Scripts

App command implementations follow a standard structure so that they can be invoked in an uniform manner. For any command, the python scripts are invoked as:

`python SCRIPT COMMAND JSON_FILE PACKAGE_ROOT STRUCTURED_OUT_FILE`

* SCRIPT is the top level script that implements the commands for the component. 

* COMMAND is one of the following default commands - START, STOP, INSTALL, CONFIG, RESTART, STATUS or any custom commands. 

* JSON_FILE includes all configuration parameters and the values. 

* PACKAGE_ROOT is the root folder of the package. From this folder, its possible to access files, scripts, templates, packages (e.g. tarballs), etc. The Yarn-App author has complete control over the structure of the package as long as the PACKAGE_ROOT and SCRIPT path is known to the management tool. 

* STRUCTURED_OUT_FILE is the file where the script can output structured data. 

The management infrastructure is expected to automatically reports back STD_OUT and STD_ERR.

Sample:

```
python /apps/HBASE_ON_YARN/package/scripts/hbase_regionserver.py START /apps/commands/cmd_332/command.json /apps/HBASE_ON_YARN/package /apps/commands/cmd_332/strout.txt
```

**Note**: The above is how Slider-Agent invokes the scripts. Its provided as a reference for developing the scripts themselves as well as a way to test/debug the scripts.

## Structure of JSON formatted parameter

The parameters are organized as multi-layer name-value pairs.

```
{
    "commandId": "Command Id as assigned by Slider",
    "command": "Command being executed",
    "commandType": "Type of command",
    "clusterName": "Name of the cluster",
    "appName": "Name of the app",
    "component": "Name of the component",
    "hostname": "Name of the host",
    "public_hostname": "FQDN of the host",
    "hostParams": {
        "host specific parameters common to all commands"
    },
    "componentParams": {
        "component specific parameters, if any"
    },
    "commandParams": {
        "command specific parameters, usually used in case of custom commands"
    },
    "configurations": {
        "app-global-config": {
        },
        "config-type-2": {
        },
        "config-type-2": {
        }
    }
}
```


## Sample configuration parameters

```
{
    "commandId": "2-2",
    "command": "START",
    "commandType": "EXECUTION_COMMAND",
    "clusterName": "c1",
    "appName": "HBASE",
    "componentName": "HBASE_MASTER",
    "hostParams": {
        "java_home": "/usr/jdk64/jdk1.7.0_45"
    },
    "componentParams": {},
    "commandParams": {},
    "hostname": "c6403.ambari.apache.org",
    "public_hostname": "c6403.ambari.apache.org",
    "configurations": {
        "hbase-log4j": {
         "log4j.threshold": "ALL",
         "log4j.rootLogger": "${hbase.root.logger}",
         "log4j.logger.org.apache.zookeeper": "INFO",
         "log4j.logger.org.apache.hadoop.hbase": "DEBUG",
         "log4j.logger.org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher": "INFO",
         "log4j.logger.org.apache.hadoop.hbase.zookeeper.ZKUtil": "INFO",
         "log4j.category.SecurityLogger": "${hbase.security.logger}",
         "log4j.appender.console": "org.apache.log4j.ConsoleAppender",
         "log4j.appender.console.target": "System.err",
         "log4j.appender.console.layout": "org.apache.log4j.PatternLayout",
         "log4j.appender.console.layout.ConversionPattern": "%d{ISO8601} %-5p [%t] %c{2}: %m%n",
         "log4j.appender.RFAS": "org.apache.log4j.RollingFileAppender",
         "log4j.appender.RFAS.layout": "org.apache.log4j.PatternLayout",
         "log4j.appender.RFAS.layout.ConversionPattern": "%d{ISO8601} %p %c: %m%n",
         "log4j.appender.RFAS.MaxFileSize": "${hbase.security.log.maxfilesize}",
         "log4j.appender.RFAS.MaxBackupIndex": "${hbase.security.log.maxbackupindex}",
         "log4j.appender.RFAS.File": "${hbase.log.dir}/${hbase.security.log.file}",
         "log4j.appender.RFA": "org.apache.log4j.RollingFileAppender",
         "log4j.appender.RFA.layout": "org.apache.log4j.PatternLayout",
         "log4j.appender.RFA.layout.ConversionPattern": "%d{ISO8601} %-5p [%t] %c{2}: %m%n",
         "log4j.appender.RFA.MaxFileSize": "${hbase.log.maxfilesize}",
         "log4j.appender.RFA.MaxBackupIndex": "${hbase.log.maxbackupindex}",
         "log4j.appender.RFA.File": "${hbase.log.dir}/${hbase.log.file}",
         "log4j.appender.NullAppender": "org.apache.log4j.varia.NullAppender",
         "log4j.appender.DRFA": "org.apache.log4j.DailyRollingFileAppender",
         "log4j.appender.DRFA.layout": "org.apache.log4j.PatternLayout",
         "log4j.appender.DRFA.layout.ConversionPattern": "%d{ISO8601} %-5p [%t] %c{2}: %m%n",
         "log4j.appender.DRFA.File": "${hbase.log.dir}/${hbase.log.file}",
         "log4j.appender.DRFA.DatePattern": ".yyyy-MM-dd",
         "log4j.additivity.SecurityLogger": "false",
         "hbase.security.logger": "INFO,console",
         "hbase.security.log.maxfilesize": "256MB",
         "hbase.security.log.maxbackupindex": "20",
         "hbase.security.log.file": "SecurityAuth.audit",
         "hbase.root.logger": "INFO,console",
         "hbase.log.maxfilesize": "256MB",
         "hbase.log.maxbackupindex": "20",
         "hbase.log.file": "hbase.log",
         "hbase.log.dir": "."
        },
        "app-global-config": {
         "security_enabled": "false",
         "pid_dir": "/hadoop/yarn/log/application_1394053491953_0003/run",
         "log_dir": "/hadoop/yarn/log/application_1394053491953_0003/log",
         "tmp_dir": "/hadoop/yarn/log/application_1394053491953_0003/tmp",
         "user_group": "hadoop",
         "user": "hbase",
         "hbase_regionserver_heapsize": "1024m",
         "hbase_master_heapsize": "1024m",
         "fs_default_name": "hdfs://c6403.ambari.apache.org:8020",
         "hdfs_root": "/apps/hbase/instances/01",
         "zookeeper_node": "/apps/hbase/instances/01",
         "zookeeper_quorom_hosts": "c6403.ambari.apache.org",
         "zookeeper_port": "2181",
        },
        "hbase-site": {
         "hbase.hstore.flush.retries.number": "120",
         "hbase.client.keyvalue.maxsize": "10485760",
         "hbase.hstore.compactionThreshold": "3",
         "hbase.rootdir": "hdfs://c6403.ambari.apache.org:8020/apps/hbase/instances/01/data",
         "hbase.stagingdir": "hdfs://c6403.ambari.apache.org:8020/apps/hbase/instances/01/staging",
         "hbase.regionserver.handler.count": "60",
         "hbase.regionserver.global.memstore.lowerLimit": "0.38",
         "hbase.hregion.memstore.block.multiplier": "2",
         "hbase.hregion.memstore.flush.size": "134217728",
         "hbase.superuser": "yarn",
         "hbase.zookeeper.property.clientPort": "2181",
         "hbase.regionserver.global.memstore.upperLimit": "0.4",
         "zookeeper.session.timeout": "30000",
         "hbase.tmp.dir": "/hadoop/yarn/log/application_1394053491953_0003/tmp",
         "hbase.hregion.max.filesize": "10737418240",
         "hfile.block.cache.size": "0.40",
         "hbase.security.authentication": "simple",
         "hbase.defaults.for.version.skip": "true",
         "hbase.zookeeper.quorum": "c6403.ambari.apache.org",
         "zookeeper.znode.parent": "/apps/hbase/instances/01",
         "hbase.hstore.blockingStoreFiles": "10",
         "hbase.hregion.majorcompaction": "86400000",
         "hbase.security.authorization": "false",
         "hbase.cluster.distributed": "true",
         "hbase.hregion.memstore.mslab.enabled": "true",
         "hbase.client.scanner.caching": "100",
         "hbase.zookeeper.useMulti": "true",
         "hbase.regionserver.info.port": "",
         "hbase.master.info.port": "60010"
        }
    }
}
```


## Sample command script

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


