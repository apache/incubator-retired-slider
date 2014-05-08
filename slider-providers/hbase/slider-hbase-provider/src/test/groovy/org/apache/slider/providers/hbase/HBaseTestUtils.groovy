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

package org.apache.slider.providers.hbase

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.RetriesExhaustedException
import org.apache.slider.common.SliderKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.exceptions.WaitTimeoutException
import org.apache.slider.test.SliderTestUtils
import org.apache.slider.common.tools.ConfigHelper
import org.apache.slider.common.tools.Duration
import org.apache.slider.client.SliderClient

/**
 * Static HBase test utils
 */
@Slf4j
@CompileStatic
class HBaseTestUtils extends SliderTestUtils {

  /**
   * Create an (unshared) HConnection talking to the hbase service that
   * Slider should be running
   * @param sliderClient slider client
   * @param clustername the name of the Slider cluster
   * @return the connection
   */
  public static HConnection createHConnection(Configuration clientConf) {
    HConnection hbaseConnection = HConnectionManager.createConnection(
        clientConf);
    return hbaseConnection
  }

  /**
   * get a string representation of an HBase cluster status
   * @param status cluster status
   * @return a summary for printing
   */
  public static String hbaseStatusToString(ClusterStatus status) {
    StringBuilder builder = new StringBuilder();
    builder << "Cluster " << status.clusterId;
    builder << " @ " << status.master << " version " << status.HBaseVersion;
    builder << "\nlive [\n"
    if (!status.servers.empty) {
      status.servers.each() { ServerName name ->
        builder << " Server " << name << " :" << status.getLoad(name) << "\n"
      }
    } else {
    }
    builder << "]\n"
    if (status.deadServers > 0) {
      builder << "\n dead servers=${status.deadServers}"
    }
    return builder.toString()
  }

  public static ClusterStatus getHBaseClusterStatus(SliderClient sliderClient) {
    Configuration clientConf = createHBaseConfiguration(sliderClient)
    return getHBaseClusterStatus(clientConf)
  }

  public static ClusterStatus getHBaseClusterStatus(Configuration clientConf) {
    try {
      HConnection hbaseConnection1 = createHConnection(clientConf)
      HConnection hbaseConnection = hbaseConnection1;
      HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConnection);
      ClusterStatus hBaseClusterStatus = hBaseAdmin.clusterStatus;
      return hBaseClusterStatus;
    } catch (NoSuchMethodError e) {
      throw new Exception("Using an incompatible version of HBase!", e);
    }
  }

  /**
   * Create an HBase config to work with
   * @param sliderClient slider client
   * @param clustername cluster
   * @return an hbase config extended with the custom properties from the
   * cluster, including the binding to the HBase cluster
   */
  public static Configuration createHBaseConfiguration(SliderClient sliderClient) {
    Configuration siteConf = fetchClientSiteConfig(sliderClient);
    Configuration conf = HBaseConfiguration.create(siteConf);
    // patch in some timeouts
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10)
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 5000)
    
    //fixed time of 1 s per attempt, not any multiplicative pause
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 100)
    conf.setInt("hbase.client.retries.longer.multiplier", 1)
    return conf
  }

  /**
   * Ask the AM for the site configuration -then dump it
   * @param sliderClient
   * @param clustername
   */
  public static void dumpHBaseClientConf(SliderClient sliderClient) {
    Configuration conf = fetchClientSiteConfig(sliderClient);
    describe("AM-generated site configuration");
    ConfigHelper.dumpConf(conf);
  }

  /**
   * Create a full HBase configuration by merging the AM data with
   * the rest of the local settings. This is the config that would
   * be used by any clients
   * @param sliderClient slider client
   * @param clustername name of the cluster
   */
  public static void dumpFullHBaseConf(SliderClient sliderClient) {
    Configuration conf = createHBaseConfiguration(sliderClient);
    describe("HBase site configuration from AM");
    ConfigHelper.dumpConf(conf);
  }

  /**
   * Wait for the hbase master to be live (or past it in the lifecycle)
   * @param clustername cluster
   * @param spintime time to wait
   * @return true if the cluster came out of the sleep time live 
   * @throws IOException
   * @throws org.apache.slider.core.exceptions.SliderException
   */
  public static boolean spinForClusterStartup(
      SliderClient sliderClient,
      long spintime,
      String role = "master")
  throws WaitTimeoutException, IOException, SliderException {
    int state = sliderClient.waitForRoleInstanceLive(role, spintime);
    return state == ClusterDescription.STATE_LIVE;
  }

  public static ClusterStatus basicHBaseClusterStartupSequence(
      SliderClient sliderClient, int startupTime, int startupToLiveTime ) {
    int state = sliderClient.waitForRoleInstanceLive(SliderKeys.COMPONENT_AM,
                                                   startupTime);
    assert state == ClusterDescription.STATE_LIVE;
    state = sliderClient.waitForRoleInstanceLive(HBaseKeys.ROLE_MASTER,
                                               startupTime);
    assert state == ClusterDescription.STATE_LIVE;
    //sleep for a bit to give things a chance to go live
    assert spinForClusterStartup(
        sliderClient,
        startupToLiveTime,
        HBaseKeys.MASTER);

    //grab the conf from the status and verify the ZK binding matches

    ClusterStatus clustat = getHBaseClusterStatus(sliderClient);
    describe("HBASE CLUSTER STATUS \n " + hbaseStatusToString(clustat));
    return clustat;
  }

  /**
   * Spin waiting for the RS count to match expected
   * @param sliderClient client
   * @param clustername cluster name
   * @param regionServerCount RS count
   * @param timeout timeout
   */
  public static ClusterStatus waitForHBaseRegionServerCount(
      SliderClient sliderClient,
      String clustername,
      int regionServerCount,
      int timeout) {
    Duration duration = new Duration(timeout);
    duration.start();
    ClusterStatus clustat = null;
    Configuration clientConf = createHBaseConfiguration(sliderClient)
    while (true) {
      clustat = getHBaseClusterStatus(clientConf);
      int workerCount = clustat.servers.size();
      if (workerCount >= regionServerCount) {
        break;
      }
      if (duration.limitExceeded) {
        describe("Cluster region server count of $regionServerCount not met:");
        log.info(hbaseStatusToString(clustat));
        ClusterDescription status = sliderClient.getClusterDescription(
            clustername);
        fail("Expected $regionServerCount YARN region servers," +
             " but  after $timeout millis saw $workerCount in ${hbaseStatusToString(clustat)}" +
             " \n ${prettyPrint(status.toJsonString())}");
      }
      log.info(
          "Waiting for $regionServerCount region servers -got $workerCount");
      Thread.sleep(1000);
    }
    return clustat;
  }

  /**
   * Probe to test for the HBasemaster being found
   * @param clientConf client configuration
   * @return true if the master was found in the hbase cluster status
   */
  public static boolean isHBaseMasterFound(Configuration clientConf) {
    HConnection hbaseConnection
    hbaseConnection = createHConnection(clientConf)
    HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConnection);
    boolean masterFound
    try {
      ClusterStatus hBaseClusterStatus = hBaseAdmin.getClusterStatus();
      masterFound = true;
    } catch (RetriesExhaustedException e) {
      masterFound = false;
    }
    return masterFound
  }

  /**
   * attempt to talk to the hbase master; expect a failure
   * @param clientConf client config
   */
  public static void assertNoHBaseMaster(
      SliderClient sliderClient,
      Configuration clientConf) {
    boolean masterFound = isHBaseMasterFound(clientConf)
    if (masterFound) {
      def text = "HBase master is unexpectedly running"
      dumpClusterStatus(sliderClient, text);
      fail(text)
    }
  }

  /**
   * attempt to talk to the hbase master; expect success
   * @param clientConf client config
   */
  public static void assertHBaseMasterFound(Configuration clientConf) {
    boolean masterFound = isHBaseMasterFound(clientConf)
    if (!masterFound) {
      fail("HBase master not running")
    }
  }
  
}
