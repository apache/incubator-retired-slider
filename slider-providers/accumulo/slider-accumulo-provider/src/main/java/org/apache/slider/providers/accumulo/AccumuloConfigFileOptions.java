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

package org.apache.slider.providers.accumulo;

/**
 * Mappings of config params to env variables for
 * custom -site.xml files to pick up
 */
public interface AccumuloConfigFileOptions {


  /**
   * quorum style, comma separated list of hostname:port values
   */
  String ZOOKEEPER_HOST = "instance.zookeeper.host";

  /**
   * URI to the filesystem
   */
  String INSTANCE_DFS_URI = "instance.dfs.uri";

  /**
   * Dir under the DFS URI
   */
  String INSTANCE_DFS_DIR = "instance.dfs.dir";

  // String used to restrict access to data in ZK
  String INSTANCE_SECRET = "instance.secret";
  
  // IPC port for master
  String MASTER_PORT_CLIENT = "master.port.client";
  String MASTER_PORT_CLIENT_DEFAULT = "9999";
  
  // IPC port for monitor
  String MONITOR_PORT_CLIENT = "monitor.port.client";
  String MONITOR_PORT_CLIENT_DEFAULT = "50095";
  int MONITOR_PORT_CLIENT_INT = Integer.parseInt(MONITOR_PORT_CLIENT_DEFAULT);
  
  // Log4j forwarding port
  String MONITOR_LOG4J_PORT = "monitor.port.log4j";
  String MONITOR_LOG4J_PORT_DEFAULT = "4560";
  int MONITOR_LOG4J_PORT_INT = Integer.parseInt(MONITOR_LOG4J_PORT_DEFAULT);
  
  // IPC port for tracer
  String TRACE_PORT_CLIENT = "trace.port.client";
  String TRACE_PORT_CLIENT_DEFAULT = "trace.port.client";

  // IPC port for tserver
  String TSERV_PORT_CLIENT = "tserver.port.client";
  String TSERV_PORT_CLIENT_DEFAULT = "tserver.port.client";
  
  // IPC port for gc
  String GC_PORT_CLIENT = "gc.port.client";
  String GC_PORT_CLIENT_DEFAULT = "50091";
  int GC_PORT_CLIENT_INT = Integer.parseInt(GC_PORT_CLIENT_DEFAULT);
}
