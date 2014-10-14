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

import groovy.json.JsonSlurper
import org.apache.accumulo.core.conf.Property
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.funtest.framework.SliderShell

class AccumuloMonitorSSLLocalCertsIT extends AccumuloMonitorSSLIT {
  protected String templateName() {
    return sysprop("test.app.resources.dir") + "/appConfig_monitor_ssl_local_certs.json"
  }

  protected ConfTree modifyTemplate(ConfTree confTree) {
    confTree = super.modifyTemplate(confTree)
    confTree.global.put("site.accumulo-site." + Property.RPC_SSL_KEYSTORE_PATH.toString(),
      clientKeyStoreFile.getAbsolutePath())
    confTree.global.put("site.accumulo-site." + Property.RPC_SSL_TRUSTSTORE_PATH.toString(),
      trustStoreFile.getAbsolutePath())
    return confTree
  }

  @Override
  public String getClusterName() {
    return "test_monitor_ssl_local_certs";
  }

  @Override
  public String getDescription() {
    return "Test enable monitor SSL with local certs $clusterName"
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    super.clusterLoadOperations(cd, sliderClient)
    File accumuloSiteFile = new File(TEST_APP_PKG_DIR, "accumulo-site.json")
    SliderShell shell = registry(EXIT_SUCCESS,
      [
        ARG_GETCONF, "accumulo-site",
        ARG_NAME, getClusterName(),
        ARG_FORMAT, "json",
        ARG_DEST, accumuloSiteFile.getAbsolutePath()
      ])

    logShell(shell)

    def slurper = new JsonSlurper()
    def siteconf = slurper.parse(accumuloSiteFile)
    assert siteconf[Property.RPC_SSL_KEYSTORE_PATH.toString()] ==
      clientKeyStoreFile.getAbsolutePath(), "unexpected keystore path"
    assert siteconf[Property.RPC_SSL_TRUSTSTORE_PATH.toString()] ==
      trustStoreFile.getAbsolutePath(), "unexpected truststore path"
  }
}
