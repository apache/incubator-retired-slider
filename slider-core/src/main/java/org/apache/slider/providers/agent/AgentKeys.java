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

package org.apache.slider.providers.agent;

/*

 */
public interface AgentKeys {

  String PROVIDER_AGENT = "agent";
  String ROLE_NODE = "node";
  /**
   * {@value}
   */
  String CONF_FILE = "agent.conf";
  /**
   * {@value}
   */
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
   * Template stored in the slider classpath -to use if there is
   * no site-specific template
   * {@value}
   */
  String CONF_RESOURCE = "org/apache/slider/providers/agent/conf/";
  /*  URL to talk back to Agent Controller*/
  String CONTROLLER_URL = "agent.controller.url";
  /**
   * The location of pre-installed agent path.
   * This can be also be dynamically computed based on Yarn installation of agent.
   */
  String PACKAGE_PATH = "agent.package.root";
  /**
   * The location of the script implementing the command.
   */
  String SCRIPT_PATH = "agent.script";
  /**
   * Execution home for the agent.
   */
  String APP_HOME = "app.home";
  /**
   * Name of the service.
   */
  String SERVICE_NAME = "app.name";
  String ARG_LABEL = "--label";
  String ARG_HOST = "--host";
  String ARG_PORT = "--port";
  String AGENT_MAIN_SCRIPT_ROOT = "./infra/agent/slider-agent/";
  String AGENT_MAIN_SCRIPT = "agent/main.py";

  String APP_DEF = "application.def";
  String AGENT_VERSION = "agent.version";
  String AGENT_CONF = "agent.conf";

  String AGENT_INSTALL_DIR = "infra/agent";
  String APP_DEFINITION_DIR = "app/definition";
  String AGENT_CONFIG_FILE = "infra/conf/agent.ini";
  String AGENT_VERSION_FILE = "infra/version";

  String JAVA_HOME = "java_home";
  String PACKAGE_LIST = "package_list";
  String COMPONENT_SCRIPT = "role.script";
  String WAIT_HEARTBEAT = "wait.heartbeat";
  String PYTHON_EXE = "python";
}


