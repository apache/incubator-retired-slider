/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.providers.accumulo;

import org.apache.slider.api.StatusKeys;

/**
 * Any keys related to acculumulo
 */
public interface AccumuloKeys {
  String PROVIDER_ACCUMULO = "accumulo";
  
  String ROLE_MASTER = "master";

  String ROLE_TABLET = "tserver";
  String ROLE_GARBAGE_COLLECTOR = "gc";
  String ROLE_MONITOR = "monitor";
  String ROLE_TRACER = "tracer";

  String OPTION_ZK_TAR = "zk.image.path";  
  String OPTION_ZK_HOME = "zk.home";  
  String OPTION_HADOOP_HOME = "hadoop.home";  
  String OPTION_ACCUMULO_PASSWORD = "accumulo.password";  

  String DEFAULT_MASTER_HEAP = "256";
  String DEFAULT_MASTER_YARN_RAM = "384";
  String DEFAULT_MASTER_YARN_VCORES = "1";
  String DEFAULT_ROLE_YARN_VCORES = "1";
  String DEFAULT_ROLE_HEAP = DEFAULT_MASTER_HEAP;
  String DEFAULT_ROLE_YARN_RAM = DEFAULT_MASTER_YARN_RAM;

// org.apache.slider.providers.accumulo.conf

  String VERSION = "version";

  String CREATE_MASTER = ROLE_MASTER;
  String CREATE_GC = ROLE_GARBAGE_COLLECTOR;
  String CREATE_TABLET = ROLE_TABLET;
  String CREATE_MONITOR = ROLE_MONITOR;
  String CREATE_TRACER  = ROLE_TRACER;


  String ACTION_START = "start";
  String ACTION_STOP = "stop";

  /**
   * Config directory : {@value}
   */
  String ARG_CONFIG = "--config";
  /**
   *  name of the hbase script relative to the hbase root dir:  {@value}
   */
  String START_SCRIPT = "bin/accumulo";

  /**
   *  name of the site conf to generate :  {@value}
   */
  String SITE_XML = "accumulo-site.xml";

  /**
   * Template stored in the slider classpath -to use if there is
   * no site-specific template
   *  {@value}
   */
  String CONF_RESOURCE = "org/apache/slider/providers/accumulo/conf/";
  String SITE_XML_RESOURCE = CONF_RESOURCE + SITE_XML;
  String ACCUMULO_HOME = "ACCUMULO_HOME";

  String ACCUMULO_CONF_DIR = "ACCUMULO_CONF_DIR";
  String ACCUMULO_LOG_DIR = "ACCUMULO_LOG_DIR";
  String ACCUMULO_GENERAL_OPTS = "ACCUMULO_GENERAL_OPTS";
  String HADOOP_HOME = "HADOOP_HOME";
  String ZOOKEEPER_HOME = "ZOOKEEPER_HOME";

  /**
   * ":"-separated list of extra jars 
   * 
   */
  String ACCUMULO_XTRAJARS = "ACCUMULO_XTRAJARS";
  String HADOOP_PREFIX = "HADOOP_PREFIX" ;
  int INIT_TIMEOUT_DEFAULT = 60000;
  /**
   * timeout in millis for init to complete
   */
  String OPTION_ACCUMULO_INIT_TIMEOUT = "accumulo.init.timeout";

  /**
   *       @Parameter(names = "--instance-name", description = "the instance name, if not provided, will prompt")
   */
  String PARAM_INSTANCE_NAME = "--instance-name";
  /**
   *     @Parameter(names = "--password", description = "set the password on the command line")
   */
  String PARAM_PASSWORD = "--password";

  String MONITOR_PAGE_JSON = "/json";
  String MONITOR_PAGE_XML = "/xml";

  String ACCUMULO_VERSION_COMMAND = "version";
  
  String MASTER_ADDRESS = StatusKeys.INFO_MASTER_ADDRESS;
  String MONITOR_ADDRESS = "monitor.address";
  String INSTANCE_ID = "instance_id";
}
