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

package org.apache.slider.common.tools

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.registry.server.services.MicroZookeeperServiceKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.core.zk.ZKIntegration
import org.apache.slider.test.KeysForTests
import org.apache.slider.test.YarnZKMiniClusterTestBase
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.Stat
import org.junit.After
import org.junit.Before
import org.junit.Test

@Slf4j
@CompileStatic
class TestZKIntegration extends YarnZKMiniClusterTestBase implements KeysForTests {


  // as the static compiler doesn't resolve consistently
  public static final String USER = KeysForTests.USERNAME
  private ZKIntegration zki

  @Before
  void createCluster() {
    Configuration conf = configuration
    def name = methodName.methodName
    File zkdir = new File("target/zk/${name}")
    FileUtil.fullyDelete(zkdir);
    conf.set(MicroZookeeperServiceKeys.KEY_ZKSERVICE_DIR, zkdir.absolutePath)
    createMicroZKCluster("-"+ name, conf)
  }

  @After
  void closeZKI() {
    zki?.close()
    zki = null;
  }

  public ZKIntegration initZKI() {
    zki = createZKIntegrationInstance(
        getZKBinding(), methodName.methodName, true, false, 5000)
    return zki
  }

  @Test
  public void testListUserClustersWithoutAnyClusters() throws Throwable {
    assertHasZKCluster()
    initZKI()
    String userPath = ZKIntegration.mkSliderUserPath(USER)
    List<String> clusters = this.zki.clusters
    assert clusters.empty
  }

  @Test
  public void testListUserClustersWithOneCluster() throws Throwable {
    assertHasZKCluster()

    initZKI()
    String userPath = ZKIntegration.mkSliderUserPath(USER)
    String fullPath = zki.createPath(userPath, "/cluster-",
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.EPHEMERAL_SEQUENTIAL)
    log.info("Ephemeral path $fullPath")
    List<String> clusters = zki.clusters
    assert clusters.size() == 1
    assert fullPath.endsWith(clusters[0])
  }

  @Test
  public void testListUserClustersWithTwoCluster() throws Throwable {
    initZKI()
    String userPath = ZKIntegration.mkSliderUserPath(USER)
    String c1 = createEphemeralChild(zki, userPath)
    log.info("Ephemeral path $c1")
    String c2 = createEphemeralChild(zki, userPath)
    log.info("Ephemeral path $c2")
    List<String> clusters = zki.clusters
    assert clusters.size() == 2
    assert (c1.endsWith(clusters[0]) && c2.endsWith(clusters[1])) ||
           (c1.endsWith(clusters[1]) && c2.endsWith(clusters[0]))
  }

  @Test
  public void testCreateAndDeleteDefaultZKPath() throws Throwable {
    MockSliderClient client = new MockSliderClient()

    String path = client.createZookeeperNodeInner("cl1", true)
    zki = client.lastZKIntegration

    String zkPath = ZKIntegration.mkClusterPath(USER, "cl1")
    assert zkPath == "/services/slider/users/" + USER + "/cl1", "zkPath must be as expected"
    assert path == zkPath
    assert zki == null, "ZKIntegration should be null."
    zki = createZKIntegrationInstance(getZKBinding(), "cl1", true, false, 5000);
    assert !zki.exists(zkPath)

    path = client.createZookeeperNodeInner("cl1", false)
    zki = client.lastZKIntegration
    assert zki 
    assert zkPath == "/services/slider/users/" + USER + "/cl1", "zkPath must be as expected"
    assert path == zkPath
    assert zki.exists(zkPath)
    zki.createPath(zkPath, "/cn", ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    assert zki.exists(zkPath + "/cn")
    client.deleteZookeeperNode("cl1")
    assert !zki.exists(zkPath)
  }

  public String createEphemeralChild(ZKIntegration zki, String userPath) {
    return zki.createPath(userPath, "/cluster-",
                          ZooDefs.Ids.OPEN_ACL_UNSAFE,
                          CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  class MockSliderClient extends SliderClient {
    private ZKIntegration zki;

    @Override
    public String getUsername() {
      return USER
    }

    @Override
    protected ZKIntegration getZkClient(String clusterName, String user) {
      zki = createZKIntegrationInstance(getZKBinding(), clusterName, true, false, 5000)
      return zki;
    }

    @Override
    public synchronized Configuration getConfig() {
      new Configuration();
    }

    public ZKIntegration getLastZKIntegration() {
      return zki
    }

  }

}
