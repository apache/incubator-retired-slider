/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.test

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.slider.core.zk.BlockingZKWatcher
import org.apache.slider.core.zk.ZKIntegration

import java.util.concurrent.atomic.AtomicBoolean

import static org.apache.slider.test.KeysForTests.*;

/**
 * Base class for mini cluster tests that use Zookeeper
 */
@CompileStatic
@Slf4j
public abstract class YarnZKMiniClusterTestBase extends YarnMiniClusterTestBase {

  protected MicroZKCluster microZKCluster
  
  public void stopMiniCluster() {
    super.stopMiniCluster()
    IOUtils.closeStream(microZKCluster);
  }

  public ZKIntegration createZKIntegrationInstance(String zkQuorum,
                                                   String clusterName,
                                                   boolean createClusterPath,
                                                   boolean canBeReadOnly,
                                                   int timeout) {
    
    BlockingZKWatcher watcher = new BlockingZKWatcher();
    ZKIntegration zki = ZKIntegration.newInstance(zkQuorum,
                                                  USERNAME,
                                                  clusterName,
                                                  createClusterPath,
                                                  canBeReadOnly, watcher) 
    zki.init()
    //here the callback may or may not have occurred.
    //optionally wait for it
    if (timeout > 0) {
      watcher.waitForZKConnection(timeout)
    }
    //if we get here, the binding worked
    log.info("Connected: ${zki}")
    return zki
  }

  /**
   * Wait for a flag to go true
   * @param connectedFlag
   */
  public void waitForZKConnection(AtomicBoolean connectedFlag, int timeout) {
    synchronized (connectedFlag) {
      if (!connectedFlag.get()) {
        log.info("waiting for ZK event")
        //wait a bit
        connectedFlag.wait(timeout)
      }
    }
    assert connectedFlag.get()
  }

  /**
   * Create and start a minicluster with ZK
   * @param name cluster/test name
   * @param conf configuration to use
   * @param noOfNodeManagers #of NMs
   * @param numLocalDirs #of local dirs
   * @param numLogDirs #of log dirs
   * @param startZK create a ZK micro cluster *THIS IS IGNORED*
   * @param startHDFS create an HDFS mini cluster
   */
  protected String createMiniCluster(String name,
                                   YarnConfiguration conf,
                                   int noOfNodeManagers,
                                   int numLocalDirs,
                                   int numLogDirs,
                                   boolean startZK,
                                   boolean startHDFS) {
    createMicroZKCluster("-${name?:methodName.methodName}", conf)
    conf.setBoolean(RegistryConstants.KEY_REGISTRY_ENABLED, true)
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_QUORUM, ZKBinding)
    //now create the cluster
    name = super.createMiniCluster(name, conf, noOfNodeManagers, numLocalDirs, numLogDirs,
        startHDFS)

    return name
  }

  /**
   * Create and start a minicluster
   * @param name cluster/test name
   * @param conf configuration to use
   * @param noOfNodeManagers #of NMs
   * @param startZK create a ZK micro cluster
   */
  protected String createMiniCluster(String name,
                                   YarnConfiguration conf,
                                   int noOfNodeManagers,
                                   boolean startZK) {
    return createMiniCluster(name, conf, noOfNodeManagers, 1, 1, startZK, false)
  }

  /**
   * Create and start a minicluster with the name from the test method
   * @param name cluster/test name
   * @param conf configuration to use
   * @param noOfNodeManagers #of NMs
   * @param startZK create a ZK micro cluster
   */
  protected String createMiniCluster(YarnConfiguration conf,
      int noOfNodeManagers,
      boolean startZK) {
    return createMiniCluster("", conf, noOfNodeManagers, 1, 1, startZK, false)
  }

  public void createMicroZKCluster(String name, Configuration conf) {
    microZKCluster = new MicroZKCluster(new Configuration(conf))
    microZKCluster.createCluster(name);
  }

  void assertHasZKCluster() {
    assert microZKCluster != null
  }

  public String getZKBinding() {
    if (!microZKCluster) {
      return "localhost:1"
    } else {
      return microZKCluster.zkBindingString
    }
  }

  /**
   * CLI args include all the ZK bindings needed
   * @return
   */
  protected List<String> getExtraCLIArgs() {
    [
      "-D", define(RegistryConstants.KEY_REGISTRY_ZK_QUORUM, ZKBinding)
    ]
  }
}
