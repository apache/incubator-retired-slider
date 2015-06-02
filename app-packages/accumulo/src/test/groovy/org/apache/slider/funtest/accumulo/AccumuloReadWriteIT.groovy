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
import org.apache.accumulo.core.cli.BatchWriterOpts
import org.apache.accumulo.core.cli.ScannerOpts
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.test.TestIngest
import org.apache.accumulo.test.VerifyIngest
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.funtest.framework.FuntestProperties

import java.util.concurrent.atomic.AtomicBoolean

@Slf4j
class AccumuloReadWriteIT extends AccumuloBasicIT {

  @Override
  public String getClusterName() {
    return "test_read_write";
  }

  @Override
  public String getDescription() {
    return "Test reading and writing to Accumulo cluster $clusterName"
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    try {
      String zookeepers = SLIDER_CONFIG.get(
          RegistryConstants.KEY_REGISTRY_ZK_QUORUM,
        FuntestProperties.DEFAULT_SLIDER_ZK_HOSTS)

      ClientConfiguration configuration = new ClientConfiguration()
      configuration.setProperty(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST, zookeepers)
      configuration.setProperty(ClientConfiguration.ClientProperty.INSTANCE_NAME, tree.global.get("site.client.instance.name"))

      ZooKeeperInstance instance = new ZooKeeperInstance(configuration)
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

  public static void ingest(Connector connector, int rows, int cols, int width, int offset) throws Exception {
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.setPrincipal(USER);
    opts.rows = rows;
    opts.cols = cols;
    opts.dataSize = width;
    opts.startRow = offset;
    opts.columnFamily = "colf";
    opts.createTable = true;
    TestIngest.ingest(connector, opts, new BatchWriterOpts());
  }

  public static void verify(Connector connector, int rows, int cols, int width, int offset) throws Exception {
    ScannerOpts scannerOpts = new ScannerOpts();
    VerifyIngest.Opts opts = new VerifyIngest.Opts();
    opts.setPrincipal(USER);
    opts.rows = rows;
    opts.cols = cols;
    opts.dataSize = width;
    opts.startRow = offset;
    opts.columnFamily = "colf";
    VerifyIngest.verifyIngest(connector, opts, scannerOpts);
  }

  public static void interleaveTest(final Connector connector) throws Exception {
    final int ROWS = 200000;
    final AtomicBoolean fail = new AtomicBoolean(false);
    final int CHUNKSIZE = ROWS / 10;
    ingest(connector, CHUNKSIZE, 1, 50, 0);
    int i;
    for (i = 0; i < ROWS; i += CHUNKSIZE) {
      final int start = i;
      Thread verify = new Thread() {
        @Override
        public void run() {
          try {
            verify(connector, CHUNKSIZE, 1, 50, start);
          } catch (Exception ex) {
            fail.set(true);
          }
        }
      };
      ingest(connector, CHUNKSIZE, 1, 50, i + CHUNKSIZE);
      verify.join();
      assertFalse(fail.get());
    }
    verify(connector, CHUNKSIZE, 1, 50, i);
  }

}
