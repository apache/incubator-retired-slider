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
package org.apache.slider.funtest.accumulo

import groovy.util.logging.Slf4j
import org.apache.accumulo.proxy.TestProxyClient
import org.apache.accumulo.proxy.thrift.AccumuloProxy
import org.apache.accumulo.proxy.thrift.ColumnUpdate
import org.apache.accumulo.proxy.thrift.TimeType
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.core.registry.docstore.PublishedConfiguration

import java.nio.ByteBuffer

@Slf4j
class AccumuloProxyIT extends AccumuloBasicIT {
  @Override
  protected Map<String, Integer> getRoleMap() {
    // must match the values in src/test/resources/resources.json
    return [
      "ACCUMULO_MASTER" : 1,
      "ACCUMULO_TSERVER" : 1,
      "ACCUMULO_MONITOR": 0,
      "ACCUMULO_GC": 0,
      "ACCUMULO_TRACER" : 0,
      "ACCUMULO_PROXY" : 2
    ];
  }

  @Override
  protected String getAppResource() {
    return sysprop("test.app.resources.dir") + "/resources_with_proxy.json"
  }

  @Override
  public String getClusterName() {
    return "test_proxy";
  }

  @Override
  public String getDescription() {
    return "Test proxy $clusterName"
  }

  def getProxies(SliderClient sliderClient, String clusterName, int expectedNumber) {
    int tries = 5
    Exception caught;
    while (true) {
      try {
        PublishedConfiguration configuration = getExport(sliderClient,
          clusterName, "componentinstancedata")

        def proxies = configuration.entries.values()
        if (proxies.size() != expectedNumber)
          throw new IllegalStateException("Didn't find all proxies")
        return proxies
      } catch (Exception e) {
        caught = e;
        log.info("Got exception trying to read proxies")
        if (tries-- == 0) {
          break
        }
        sleep(20000)
      }
    }
    throw caught;
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    def proxies = getProxies(sliderClient, getClusterName(), 2)
    for (int i = 0; i < proxies.size(); i++) {
      log.info("Checking proxy " + proxies[i])
      def hostPort = proxies[i].split(":")

      // create proxy client
      AccumuloProxy.Client client = new TestProxyClient(hostPort[0],
        hostPort[1].toInteger()).proxy()
      ByteBuffer token = client.login("root",
        Collections.singletonMap("password", PASSWORD))

      // verify table list before
      String tableName = "table" + i
      Set<String> tablesBefore = client.listTables(token)
      assertFalse(tablesBefore.contains(tableName))

      // create table, verify its contents, write entry, verify again
      client.createTable(token, tableName, true, TimeType.MILLIS)
      String scanner = client.createScanner(token, tableName, null)
      assertFalse(client.hasNext(scanner))
      client.updateAndFlush(token, tableName, createUpdate("row1", "cf1",
        "cq1", "val1"))
      scanner = client.createScanner(token, tableName, null)
      assertTrue(client.hasNext(scanner))
      client.nextEntry(scanner)
      assertFalse(client.hasNext(scanner))

      // verify table list after
      Set<String> tablesAfter = client.listTables(token)
      assertTrue(tablesAfter.contains(tableName))
      assertEquals(tablesBefore.size() + 1, tablesAfter.size())
    }
  }

  def createUpdate(String row, String cf, String cq, String value) {
    ColumnUpdate update = new ColumnUpdate(wrap(cf), wrap(cq));
    update.setValue(value.getBytes());
    return Collections.singletonMap(wrap(row), Collections.singletonList(update));
  }

  def wrap(String cf) {
    return ByteBuffer.wrap(cf.getBytes("UTF-8"));
  }

}
