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

package org.apache.slider.common.params;

/**
 * Here are all the arguments that may be parsed by the client or server
 * command lines. 
 */
public interface Arguments {

  String ARG_APP_HOME = "--apphome";
  String ARG_CONFDIR = "--appconf";
  String ARG_COMPONENT = "--component";
  String ARG_COMPONENT_SHORT = "--comp";
  String ARG_COMP_OPT= "--compopt";
  String ARG_COMP_OPT_SHORT = "--co";
  
  String ARG_DEBUG = "--debug";
  String ARG_DEST = "--dest";
  String ARG_DEFINE = "-D";
  String ARG_EXITCODE = "--exitcode";
  /**
   filesystem-uri: {@value}
   */
  String ARG_FILESYSTEM = "--fs";
  String ARG_FILESYSTEM_LONG = "--filesystem";
  String ARG_BASE_PATH = "--basepath";
  String ARG_FORMAT = "--format";
  String ARG_FORCE = "--force";
  String ARG_GETCONF = "--getconf";
  String ARG_GETFILES = "--getfiles";
  String ARG_HELP = "--help";
  String ARG_ID = "--id";
  String ARG_IMAGE = "--image";
  String ARG_LIST = "--list";
  String ARG_LISTFILES = "--listfiles";
  String ARG_LISTCONF = "--listconf";
  String ARG_LIVE = "--live";
  String ARG_MANAGER = "--manager";
  String ARG_MANAGER_SHORT = "--m";
  String ARG_MESSAGE = "--message";
  String ARG_OPTION = "--option";
  String ARG_OPTION_SHORT = "-O";
  //  String ARG_NAME = "--name";
  String ARG_OUTPUT = "--out";
  String ARG_OUTPUT_SHORT = "-o";
  String ARG_PACKAGE = "--package";
  String ARG_PROVIDER = "--provider";
  String ARG_RESOURCES = "--resources";
  String ARG_RES_COMP_OPT = "--rescompopt";
  String ARG_RES_COMP_OPT_SHORT = "--rco";
  String ARG_RESOURCE_MANAGER = "--rm";
  String ARG_RESOURCE_OPT = "--resopt";
  String ARG_RESOURCE_OPT_SHORT = "-ro";
  String ARG_SYSPROP = "-S";
  String ARG_TEMPLATE = "--template";
  String ARG_WAIT = "--wait";
  String ARG_ZKPATH = "--zkpath";
  String ARG_ZKPORT = "--zkport";
  String ARG_ZKHOSTS = "--zkhosts";


  /**
   * Deprecated
   */
  @Deprecated
  String ARG_ROLE = "--role";
  @Deprecated
  String ARG_ROLEOPT = "--roleopt";

  /**
   * server: URI for the cluster
   */
  String ARG_CLUSTER_URI = "-cluster-uri";


  /**
   * server: Path for the resource manager instance (required)
   */
  String ARG_RM_ADDR = "--rm";

  String FORMAT_XML = "xml";
  String FORMAT_PROPERTIES = "properties";

}
