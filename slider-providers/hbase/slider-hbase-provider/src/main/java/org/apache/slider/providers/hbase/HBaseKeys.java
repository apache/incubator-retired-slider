/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.providers.hbase;

public interface HBaseKeys {

  /** {@value */
  String MASTER = "master";
  String ROLE_WORKER = "worker";
  
  String ROLE_MASTER = MASTER;

  /** {@value */
  String REGION_SERVER = "regionserver";

  /**
   * What is the command for hbase to print a version: {@value}
   */
  String COMMAND_VERSION = "version";

  String ACTION_START = "start";
  String ACTION_STOP = "stop";

  /**
   * Config directory : {@value}
   */
  String ARG_CONFIG = "--config";
  /**
   *  name of the hbase script relative to the hbase root dir:  {@value}
   */
  String HBASE_SCRIPT = "hbase";
  
  /**
   *  name of the site conf to generate :  {@value}
   */
  String SITE_XML = "hbase-site.xml";
  /**
   * Template stored in the slider classpath -to use if there is
   * no site-specific template
   *  {@value}
   */
  String HBASE_CONF_RESOURCE = "org/apache/slider/providers/hbase/conf/";
  String HBASE_TEMPLATE_RESOURCE = HBASE_CONF_RESOURCE + SITE_XML;


  String DEFAULT_HBASE_WORKER_HEAP = "512M";
  String DEFAULT_HBASE_MASTER_HEAP = "512M";
  String DEFAULT_HBASE_WORKER_INFOPORT = "0";
  String DEFAULT_HBASE_MASTER_INFOPORT = "0";
  String PROVIDER_HBASE = "hbase";
  String HBASE_LOG_DIR = "HBASE_LOG_DIR";

}


