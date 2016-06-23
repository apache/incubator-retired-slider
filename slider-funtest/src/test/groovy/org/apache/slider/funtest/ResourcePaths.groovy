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
  String COMMAND_LOG_APPCONFIG_NO_HB = "$SLIDER_CORE_APP_PACKAGES/test_command_log/appConfig_no_hb.json"
  String COMMAND_LOG_APPCONFIG_FAST_NO_REG = "$SLIDER_CORE_APP_PACKAGES/test_command_log/appConfig_fast_no_reg.json"

  String PING_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/nc_ping_cmd/resources.json"
  String PING_META = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/nc_ping_cmd/metainfo.json"
  String PING_APPCONFIG = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/nc_ping_cmd/appConfig.json"

  String SLEEP_RESOURCES = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/resources.json"
  String SLEEP_META = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/metainfo.json"
  String SLEEP_APPCONFIG = "$SLIDER_CORE_APP_PACKAGES/test_min_pkg/sleep_cmd/appConfig.json"

}