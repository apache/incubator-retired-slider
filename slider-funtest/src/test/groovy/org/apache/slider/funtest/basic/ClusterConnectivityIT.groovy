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

package org.apache.slider.funtest.basic

import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.client.SliderYarnClientImpl
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.core.zk.ZookeeperUtils
import org.apache.slider.funtest.framework.CommandTestBase
import org.junit.BeforeClass
import org.junit.Test

@Slf4j
/**
 * Test basic connectivity with the target cluster, including 
 * HDFS, YARN and ZK
 */
class ClusterConnectivityIT extends CommandTestBase {


  public static final int CONNECT_TIMEOUT = 2000

  @Test
  public void testFileSystemUp() throws Throwable {

    def fs = clusterFS
    def status = fs.listStatus(new Path("/"))
    status.each {
      log.info("${it.path} = ${it}")
    }
    
  }

  @Test
  public void testZKBinding() throws Throwable {
    def quorum = SLIDER_CONFIG.getTrimmed(
        RegistryConstants.KEY_REGISTRY_ZK_QUORUM)
    assert quorum
    def tuples = ZookeeperUtils.splitToHostsAndPortsStrictly(quorum);
    tuples.each {
      telnet(it.hostText, it.port)
    }
    
  }

  @Test
  public void testRMTelnet() throws Throwable {
    def isHaEnabled = SLIDER_CONFIG.getTrimmed(YarnConfiguration.RM_HA_ENABLED)
    // Telnet test is not straight forward for HA setup and is not required
    // as long as RM binding is tested
    if (!isHaEnabled) {
      def rmAddr = SLIDER_CONFIG.getSocketAddr(YarnConfiguration.RM_ADDRESS, "", 0)
      telnet(rmAddr.hostName, rmAddr.port)
    }
  }
  
  @Test
  public void testRMBinding() throws Throwable {
    SliderYarnClientImpl yarnClient = new SliderYarnClientImpl()
    try {
      SLIDER_CONFIG.setInt("ipc.client.connect.retry.interval",100)
      SLIDER_CONFIG.setInt(
          YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,5000)
      SLIDER_CONFIG.setInt(
          YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,50)
      
      yarnClient.init(SLIDER_CONFIG)
      yarnClient.start();
      def instances = yarnClient.listInstances("")
      instances.each {it -> log.info("Instance $it.applicationId")}
    } finally {
      yarnClient.stop()
    }
  }
  
  def telnet(String host, int port) {
    assert host != ""
    assert port != 0
    try {
      def socket = new Socket();
      def addr = new InetSocketAddress(host, port)
      socket.connect(addr, CONNECT_TIMEOUT)
      socket.close()
    } catch (IOException e) {
      throw NetUtils.wrapException(host, port, "localhost", 0, e)
    }

  }
  
}
