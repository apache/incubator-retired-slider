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

import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.funtest.framework.FuntestProperties

import static org.apache.slider.funtest.accumulo.AccumuloReadWriteIT.ingest
import static org.apache.slider.funtest.accumulo.AccumuloReadWriteIT.interleaveTest
import static org.apache.slider.funtest.accumulo.AccumuloReadWriteIT.verify

class AccumuloReadWriteSSLIT extends AccumuloSSLTestBase {
  @Override
  public String getClusterName() {
    return "test_read_write_ssl";
  }

  @Override
  public String getDescription() {
    return "Test reading and writing to Accumulo cluster SSL $clusterName"
  }

  public ZooKeeperInstance getInstance() {
    String zookeepers = SLIDER_CONFIG.get(SliderXmlConfKeys.REGISTRY_ZK_QUORUM,
      FuntestProperties.DEFAULT_SLIDER_ZK_HOSTS)
    ClientConfiguration conf = new ClientConfiguration()
      .withInstance(tree.global.get("site.global.accumulo_instance_name"))
      .withZkHosts(zookeepers)
      .withSsl(true)
      .withKeystore(clientKeyStoreFile.toString(), KEY_PASS, null)
      .withTruststore(trustStoreFile.toString(), TRUST_PASS, null)
    return new ZooKeeperInstance(conf)
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    try {
      ZooKeeperInstance instance = getInstance()
      Connector connector = instance.getConnector(USER, new PasswordToken(PASSWORD))

      ingest(connector, 200000, 1, 50, 0);
      verify(connector, 200000, 1, 50, 0);

      ingest(connector, 2, 1, 500000, 0);
      verify(connector, 2, 1, 500000, 0);

      interleaveTest(connector);
    } catch (Exception e) {
      fail("Got exception connecting/reading/writing "+e)
    }
  }
}
