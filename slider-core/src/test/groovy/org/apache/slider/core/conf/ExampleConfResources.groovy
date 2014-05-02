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

package org.apache.slider.core.conf

import org.apache.slider.core.persist.JsonSerDeser
import org.apache.slider.providers.slideram.SliderAMClientProvider

/*
  names of the example configs
 */

class ExampleConfResources {

  static final String overridden = "overridden.json"
  static final String overriddenRes = "overridden-resolved.json"
  static final String internal = "internal.json"
  static final String internalRes = "internal-resolved.json"
  static final String app_configuration = "app_configuration.json"
  final
  static String app_configurationRes = "app_configuration-resolved.json"
  static final String resources = "resources.json"
  static final String empty = "empty.json"

  static final String PACKAGE = "/org/apache/slider/core/conf/examples/"


  static
  final String[] all_examples = [overridden, overriddenRes, internal, internalRes,
                                 app_configuration, app_configurationRes, resources, empty];

  static final List<String> all_example_resources = [];
  static {
    all_examples.each { all_example_resources << (PACKAGE + it) }

    all_example_resources <<
        SliderAMClientProvider.RESOURCES_JSON <<
        SliderAMClientProvider.INTERNAL_JSON << 
        SliderAMClientProvider.APPCONF_JSON
    
  }

  /**
   * Build up an aggregate conf by loading in the details of the individual resources
   * and then aggregating them
   * @return a new instance
   */
  static AggregateConf loadExampleAggregateResource() {
    JsonSerDeser<ConfTree> confTreeJsonSerDeser =
        new JsonSerDeser<ConfTree>(ConfTree)
    ConfTree internal = confTreeJsonSerDeser.fromResource(PACKAGE + internal)
    ConfTree app_conf = confTreeJsonSerDeser.fromResource(PACKAGE + app_configuration)
    ConfTree resources = confTreeJsonSerDeser.fromResource(PACKAGE + resources)
    AggregateConf aggregateConf = new AggregateConf(
        resources,
        app_conf,
        internal)
    return aggregateConf;
  }
  
  static ConfTree loadResource(String name) {
    JsonSerDeser<ConfTree> confTreeJsonSerDeser =
        new JsonSerDeser<ConfTree>(ConfTree)
    return confTreeJsonSerDeser.fromResource(PACKAGE + name)
  }
}
