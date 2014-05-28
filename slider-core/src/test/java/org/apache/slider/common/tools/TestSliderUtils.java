/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.common.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;

/**
 *
 */
public class TestSliderUtils {
  protected static final Logger log =
      LoggerFactory.getLogger(TestSliderUtils.class);

  @Test
  public void testGetMetaInfoStreamFromZip () throws Exception {
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.getLocal(configuration);
    log.info("fs working dir is {}", fs.getWorkingDirectory().toString());
    SliderFileSystem sliderFileSystem = new SliderFileSystem(fs, configuration);

    InputStream stream = SliderUtils.getApplicationResourceInputStream(
        sliderFileSystem.getFileSystem(),
        new Path("target/test-classes/org/apache/slider/common/tools/test.zip"),
        "metainfo.xml");
    Assert.assertTrue(stream != null);
    Assert.assertTrue(stream.available() > 0);
  }

  @Test
  public void testTruncate () {
    Assert.assertEquals(SliderUtils.truncate(null, 5), null);
    Assert.assertEquals(SliderUtils.truncate("323", -1), "323");
    Assert.assertEquals(SliderUtils.truncate("3232", 5), "3232");
    Assert.assertEquals(SliderUtils.truncate("1234567890", 0), "1234567890");
    Assert.assertEquals(SliderUtils.truncate("123456789012345", 15), "123456789012345");
    Assert.assertEquals(SliderUtils.truncate("123456789012345", 14), "12345678901...");
    Assert.assertEquals(SliderUtils.truncate("1234567890", 1), "1");
    Assert.assertEquals(SliderUtils.truncate("1234567890", 10), "1234567890");
    Assert.assertEquals(SliderUtils.truncate("", 10), "");
  }
}
