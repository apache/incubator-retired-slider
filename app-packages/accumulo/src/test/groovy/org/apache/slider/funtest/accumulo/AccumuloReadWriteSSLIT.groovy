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
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.funtest.framework.FuntestProperties

import static org.apache.slider.funtest.accumulo.AccumuloReadWriteIT.ingest
import static org.apache.slider.funtest.accumulo.AccumuloReadWriteIT.interleaveTest
import static org.apache.slider.funtest.accumulo.AccumuloReadWriteIT.verify

class AccumuloReadWriteSSLIT extends AccumuloSSLTestBase {
  protected static final File trustStoreFile = new File(TEST_APP_PKG_DIR, "truststore.p12")
  protected static final File clientKeyStoreFile = new File(TEST_APP_PKG_DIR, "keystore.p12")
  public static final String STORE_TYPE = "PKCS12"

  @Override
  public String getClusterName() {
    return "test_read_write_ssl";
  }

  @Override
  public String getDescription() {
    return "Test reading and writing to Accumulo cluster SSL $clusterName"
  }

  public ZooKeeperInstance getInstance() {
    String zookeepers = SLIDER_CONFIG.get(
        RegistryConstants.KEY_REGISTRY_ZK_QUORUM,
      FuntestProperties.DEFAULT_SLIDER_ZK_HOSTS)
    ClientConfiguration conf = new ClientConfiguration()
      .withInstance(tree.global.get("site.client.instance.name"))
      .withZkHosts(zookeepers)
      .withSsl(true)
      .withKeystore(clientKeyStoreFile.toString(), KEY_PASS, STORE_TYPE)
      .withTruststore(trustStoreFile.toString(), TRUST_PASS, STORE_TYPE)
    return new ZooKeeperInstance(conf)
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    slider(EXIT_SUCCESS,
      [
        ACTION_CLIENT,
        ARG_GETCERTSTORE,
        ARG_KEYSTORE, clientKeyStoreFile.getCanonicalPath(),
        ARG_NAME, getClusterName(),
        ARG_PASSWORD, KEY_PASS
      ])

    slider(EXIT_SUCCESS,
      [
        ACTION_CLIENT,
        ARG_GETCERTSTORE,
        ARG_TRUSTSTORE, trustStoreFile.getCanonicalPath(),
        ARG_NAME, getClusterName(),
        ARG_PASSWORD, TRUST_PASS
      ])

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
