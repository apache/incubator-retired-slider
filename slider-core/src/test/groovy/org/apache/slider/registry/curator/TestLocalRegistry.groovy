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

package org.apache.slider.registry.curator

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.registry.info.ServiceInstanceData
import org.apache.slider.server.services.curator.CuratorHelper
import org.apache.slider.server.services.curator.RegistryBinderService
import org.apache.slider.server.services.curator.RegistryNaming
import org.apache.slider.test.MicroZKCluster
import org.apache.slider.test.SliderTestUtils
import org.junit.After
import org.junit.Before
import org.junit.Test

class TestLocalRegistry {
  MicroZKCluster miniZK
  RegistryBinderService<ServiceInstanceData> registryBinder

  @Before
  void setup() {
    miniZK = new MicroZKCluster()
    miniZK.createCluster()

    registryBinder = createRegistry()
  }

  def RegistryBinderService<ServiceInstanceData> createRegistry(
                                                               ) {
    def conf = new YarnConfiguration()
    CuratorHelper curatorHelper =
        new CuratorHelper(conf, miniZK.zkBindingString);

    def registry
    registry = curatorHelper.createRegistryBinderService("/services");
    registry.init(conf)
    registry.start()
    return registry
  }

  @After
  void teardown() {
    registryBinder?.stop()
    miniZK?.close()
  }


  @Test
  public void testRegisterAndQuery() throws Throwable {
    registryBinder.register(SliderKeys.APP_TYPE, "instance3",
        new URL("http", "localhost", 80, "/"),
        null)
    def instance = registryBinder.queryForInstance(
        SliderKeys.APP_TYPE,
        "instance3")
    assert instance != null
  }

  @Test
  public void testRegisterAndList() throws Throwable {
    registryBinder.register(SliderKeys.APP_TYPE, "instance3",
        new URL("http", "localhost", 80, "/"),
        null)
    registryBinder.register(SliderKeys.APP_TYPE, "instance2",
        new URL("http", "localhost", 8090, "/"),
        null)
    def instances = registryBinder.instanceIDs(SliderKeys.APP_TYPE)
    assert instances.size() ==2
    def instance = registryBinder.queryForInstance(
        SliderKeys.APP_TYPE,
        "instance3")
    assert instance != null
  }

  @Test
  public void testMultipleRegistryBinders() throws Throwable {
    registryBinder.register(SliderKeys.APP_TYPE, "instance3",
        new URL("http", "localhost", 80, "/"),
        null)
    registryBinder.register(SliderKeys.APP_TYPE, "instance2",
        new URL("http", "localhost", 8090, "/"),
        null)
    RegistryBinderService<ServiceInstanceData> registry2 = createRegistry()
    RegistryBinderService<ServiceInstanceData> registry3 = createRegistry()
    try {
      def instances = registry3.instanceIDs(SliderKeys.APP_TYPE)
      assert instances.size() == 2
      def instance = registryBinder.queryForInstance(
          SliderKeys.APP_TYPE,
          "instance3")
      assert instance.id == "instance3"
      assert instance.name == SliderKeys.APP_TYPE
    } finally {
      registry3.stop()
      registry2.stop()
      
    }
  }

  @Test
  public void testNamingPolicy() throws Throwable {

    String hobbitName = RegistryNaming.createRegistryName("hobbiton",
        "bilbo",
        SliderKeys.APP_TYPE);
    String hobbitId =
        RegistryNaming.createUniqueInstanceId(
            "hobbiton",
            "bilbo",
            SliderKeys.APP_TYPE,
            1);
    String mordorName = RegistryNaming.createRegistryName("mordor",
        "bilbo",
        SliderKeys.APP_TYPE);
    String mordorId =
        RegistryNaming.createUniqueInstanceId(
            "mordor",
            "bilbo",
            SliderKeys.APP_TYPE,
            1);
    
    // service have same name
    assert hobbitName == mordorName;
    assert mordorId != hobbitId;
    registryBinder.register(mordorName, mordorId,
        new URL("http", "localhost", 8090, "/"),
        new ServiceInstanceData())
    registryBinder.register(hobbitName, hobbitId,
        new URL("http", "localhost", 80, "/mordor"),
        new ServiceInstanceData())
    def mordorInstance = registryBinder.queryForInstance(
        mordorName,
        mordorId)
    assert mordorInstance.port == 8090

    def instances = registryBinder.listInstances(SliderKeys.APP_TYPE);
    SliderTestUtils.dumpRegistryInstances(instances)
    assert instances.size() == 2
    
  }

}
