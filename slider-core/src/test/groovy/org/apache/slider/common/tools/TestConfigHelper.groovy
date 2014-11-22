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

package org.apache.slider.common.tools

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.test.YarnMiniClusterTestBase
import org.junit.Test

@Slf4j
@CompileStatic

class TestConfigHelper extends YarnMiniClusterTestBase {


  @Test
  public void testConfigLoaderIteration() throws Throwable {

    String xml =
    """<?xml version="1.0" encoding="UTF-8" standalone="no"?><configuration>
<property><name>key</name><value>value</value><source>programatically</source></property>
</configuration>
    """
    InputStream ins = new ByteArrayInputStream(xml.bytes);
    Configuration conf = new Configuration(false);
    conf.addResource(ins);
    Configuration conf2 = new Configuration(false);
    for (Map.Entry<String, String> entry : conf) {
      conf2.set(entry.key, entry.value, "src")
    }
    
  }

  @Test
  public void testConfigDeprecation() throws Throwable {
    ConfigHelper.registerDeprecatedConfigItems()
    Configuration conf = new Configuration(false);
    conf.set(SliderXmlConfKeys.REGISTRY_PATH, "path")
    assert "path" == conf.get(SliderXmlConfKeys.REGISTRY_PATH)
    assert "path" == conf.get(RegistryConstants.KEY_REGISTRY_ZK_ROOT)

    conf.set(SliderXmlConfKeys.REGISTRY_ZK_QUORUM, "localhost")
    assert "localhost" == conf.get(SliderXmlConfKeys.REGISTRY_ZK_QUORUM)
    assert "localhost" == conf.get(RegistryConstants.KEY_REGISTRY_ZK_QUORUM)
  }
}
