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
 * Actions.
 * Only some of these are supported by specific Slider Services; they
 * are listed here to ensure the names are consistent
 */
public interface SliderActions {
  String ACTION_AM_SUICIDE = "am-suicide";
  String ACTION_BUILD = "build";
  String ACTION_CREATE = "create";
  String ACTION_DESTROY = "destroy";
  String ACTION_ECHO = "echo";
  String ACTION_EXISTS = "exists";
  String ACTION_FLEX = "flex";
  String ACTION_FREEZE = "freeze";
  String ACTION_GETCONF = "getconf";
  String ACTION_HELP = "help";
  String ACTION_KILL_CONTAINER = "kill-container";
  String ACTION_LIST = "list";
  String ACTION_PREFLIGHT = "preflight";
  String ACTION_RECONFIGURE = "reconfigure";
  String ACTION_REGISTRY = "registry";
  String ACTION_STATUS = "status";
  String ACTION_THAW = "thaw";
  String ACTION_USAGE = "usage";
  String ACTION_VERSION = "version";
  String DESCRIBE_ACTION_AM_SUICIDE =
    "Tell the Slider Application Master to simulate a process failure by terminating itself";
  String DESCRIBE_ACTION_BUILD =
    "Build a Slider cluster specification -but do not start it";
  String DESCRIBE_ACTION_CREATE =
      "Create a live Slider application";
  String DESCRIBE_ACTION_DESTROY =
        "Destroy a frozen Slider application)";
  String DESCRIBE_ACTION_EXISTS =
            "Probe for an application running";
  String DESCRIBE_ACTION_FLEX = "Flex a Slider application";
  String DESCRIBE_ACTION_FREEZE =
              "Freeze/suspend a running application";
  String DESCRIBE_ACTION_GETCONF =
                "Get the configuration of an application";
  String DESCRIBE_ACTION_KILL_CONTAINER =
    "Kill a container in the application";
  String DESCRIBE_ACTION_HELP = "Print help information";
  String DESCRIBE_ACTION_LIST =
                  "List running Slider applications";
  String DESCRIBE_ACTION_MONITOR =
                    "Monitor a running application";
  String DESCRIBE_ACTION_REGISTRY =
                      "Query the registry of a YARN application";
  String DESCRIBE_ACTION_STATUS =
                      "Get the status of an application";
  String DESCRIBE_ACTION_THAW =
                        "Thaw a frozen application";
  String DESCRIBE_ACTION_VERSION =
                        "Print the Slider version information";
}
