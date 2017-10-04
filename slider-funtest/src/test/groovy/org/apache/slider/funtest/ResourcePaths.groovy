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
package org.apache.slider.funtest

/**
 * The various resources used for test runs
 */
interface ResourcePaths {

  String SLIDER_CORE_TEST_SRC= "../slider-core/src/test"
  String SLIDER_CORE_APP_PACKAGES = "$SLIDER_CORE_TEST_SRC/app_packages"
  String COMMAND_LOG_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources.json"
  String COMMAND_LOG_RESOURCES_QUEUE_LABELS = "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources_queue_labels.json"
  String COMMAND_LOG_RESOURCES_NO_ROLE = "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources_no_role.json"
  String COMMAND_LOG_RESOURCES_HEALTH_MONITOR_60 =
    "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources_health_monitor_60.json"
  String COMMAND_LOG_RESOURCES_HEALTH_MONITOR_80 =
    "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources_health_monitor_80.json"
  String COMMAND_LOG_RESOURCES_HEALTH_MONITOR_UNIQUE_NAMES_60 =
    "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources_health_monitor_uniq_names_60.json"
  String COMMAND_LOG_RESOURCES_HEALTH_MONITOR_UNIQUE_NAMES_80 =
    "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources_health_monitor_uniq_names_80.json"
  String COMMAND_LOG_RESOURCES_HEALTH_MONITOR_LOTS_OF_CONTAINERS =
    "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources_health_monitor_lots_of_containers.json"
  String COMMAND_LOG_APPCONFIG_NO_HB = "$SLIDER_CORE_APP_PACKAGES/test_command_log/appConfig_no_hb.json"
  String COMMAND_LOG_APPCONFIG_FAST_NO_REG = "$SLIDER_CORE_APP_PACKAGES/test_command_log/appConfig_fast_no_reg.json"

  String PING_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/nc_ping_cmd/resources.json"
  String PING_META = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/nc_ping_cmd/metainfo.json"
  String PING_APPCONFIG = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/nc_ping_cmd/appConfig.json"

  String SLEEP_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/resources.json"
  String SLEEP_META = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/metainfo.json"
  String SLEEP_APPCONFIG = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/appConfig.json"

  String AM_CONFIG_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_am_config/resources.json"
  String AM_CONFIG_META = "$SLIDER_CORE_APP_PACKAGES/test_am_config/metainfo.json"
  String AM_CONFIG_APPCONFIG = "$SLIDER_CORE_APP_PACKAGES/test_am_config/appConfig.json"

  String UNIQUE_COMPONENT_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_command_log/resources_unique_names.json"

  String EXTERNAL_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/resources_external_component.json"
  String EXTERNAL_APPCONFIG = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/appConfig_external_component.json"

  String NESTED_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/resources_external_component_nested.json"
  String NESTED_META = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/metainfo_external_component_nested.json"
  String NESTED_APPCONFIG = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/appConfig_external_component_nested.json"
}
