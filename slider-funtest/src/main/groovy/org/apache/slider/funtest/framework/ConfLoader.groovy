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

package org.apache.slider.funtest.framework

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration

public class ConfLoader {

  YarnConfiguration loadSliderConf(File confFile) {
    URI confURI = confFile.toURI();
    YarnConfiguration conf = new YarnConfiguration()
    def confXmlUrl = confURI.toURL()
    conf.addResource(confXmlUrl)
    conf.set(FuntestProperties.KEY_TEST_CONF_XML, confXmlUrl.toString())
    conf.set(FuntestProperties.KEY_TEST_CONF_DIR, CommandTestBase.SLIDER_CONF_DIRECTORY.absolutePath)
    return conf
  }
}
