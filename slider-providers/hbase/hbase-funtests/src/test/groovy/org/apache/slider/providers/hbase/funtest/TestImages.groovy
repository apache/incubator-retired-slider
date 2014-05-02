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

package org.apache.slider.providers.hbase.funtest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.junit.Before
import org.junit.Test

class TestImages extends CommandTestBase implements FuntestProperties {


  @Before
  public void verifyPreconditions() {
    assumeBoolOption(SLIDER_CONFIG, KEY_SLIDER_FUNTESTS_ENABLED, true)
    assumeBoolOption(SLIDER_CONFIG, KEY_TEST_HBASE_ENABLED, true)
  }
  
  @Test
  public void testImageExists() throws Throwable {

    Configuration conf = loadSliderConf()
    String testImage = conf.get(KEY_TEST_HBASE_TAR)
    assert testImage
    Path path = new Path(testImage)
    HadoopFS fs = HadoopFS.get(
        path.toUri(),
        conf)
    assert fs.exists(path)
  }

  @Test
  public void testAppConfExists() throws Throwable {
    Configuration conf = loadSliderConf()
    String dir = conf.get(KEY_TEST_HBASE_APPCONF)

    assert conf.get(KEY_TEST_HBASE_APPCONF)
    Path path = new Path(dir)
    HadoopFS fs = HadoopFS.get(
        path.toUri(),
        conf)
    assert fs.exists(path)
  }


}
