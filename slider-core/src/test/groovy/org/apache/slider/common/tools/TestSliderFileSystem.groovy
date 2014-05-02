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
package org.apache.slider.common.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.test.SliderTestBase
import org.junit.Test

class TestSliderFileSystem extends SliderTestBase {
  private static Configuration defaultConfiguration() {
    new Configuration()
  }

  private static Configuration createConfigurationWithKV(String key, String value) {
    def conf = defaultConfiguration()
    conf.set(key, value)
    conf
  }

  @Test
  public void testSliderBasePathDefaultValue() throws Throwable {
    Configuration configuration = defaultConfiguration()
    FileSystem fileSystem = FileSystem.get(configuration)

    def fs2 = new SliderFileSystem(fileSystem, configuration)
    fs2.baseApplicationPath == new Path(fileSystem.homeDirectory, ".slider")
  }

  @Test
  public void testSliderBasePathCustomValue() throws Throwable {
    Configuration configuration = createConfigurationWithKV(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH, "/slider/cluster")
    FileSystem fileSystem = FileSystem.get(configuration)
    def fs2 = new SliderFileSystem(fileSystem, configuration)

    fs2.baseApplicationPath == new Path("/slider/cluster")
  }

}
