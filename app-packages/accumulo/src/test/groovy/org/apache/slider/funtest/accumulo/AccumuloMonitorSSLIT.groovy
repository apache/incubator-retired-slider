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
package org.apache.slider.funtest.accumulo

import groovy.util.logging.Slf4j
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.core.conf.ConfTree

@Slf4j
class AccumuloMonitorSSLIT extends AccumuloSSLTestBase {
  protected String templateName() {
    return sysprop("test.app.resources.dir") + "/appConfig_monitor_ssl.json"
  }

  protected ConfTree modifyTemplate(ConfTree confTree) {
    confTree.global.put("site.global.monitor_protocol", "https")
    confTree.global.put("site.accumulo-site.monitor.ssl.keyStore",
      confTree.global.get("site.accumulo-site.rpc.javax.net.ssl.keyStore"))
    confTree.global.put("site.accumulo-site.monitor.ssl.keyStoreType",
      confTree.global.get("site.accumulo-site.rpc.javax.net.ssl.keyStoreType"))
    confTree.global.put("site.accumulo-site.monitor.ssl.trustStore",
      confTree.global.get("site.accumulo-site.rpc.javax.net.ssl.trustStore"))
    confTree.global.put("site.accumulo-site.monitor.ssl.trustStoreType",
      confTree.global.get("site.accumulo-site.rpc.javax.net.ssl.trustStoreType"))
    String jks = confTree.global.get(PROVIDER_PROPERTY)
    def keys = confTree.credentials.get(jks)
    keys.add("monitor.ssl.keyStorePassword")
    keys.add("monitor.ssl.trustStorePassword")
    return confTree
  }

  @Override
  public String getClusterName() {
    return "test_monitor_ssl";
  }

  @Override
  public String getDescription() {
    return "Test enable monitor SSL $clusterName"
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    String monitorUrl = getMonitorUrl(sliderClient, getClusterName())
    assert monitorUrl.startsWith("https://"), "Monitor URL didn't have expected protocol"
    checkMonitorPage(monitorUrl)
  }
}
