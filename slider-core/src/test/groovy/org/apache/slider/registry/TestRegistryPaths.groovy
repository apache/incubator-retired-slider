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

package org.apache.slider.registry

import org.apache.hadoop.registry.client.binding.RegistryUtils
import org.apache.slider.core.registry.SliderRegistryUtils
import org.apache.slider.test.SliderTestUtils
import org.junit.Assert
import org.junit.Test

class TestRegistryPaths {

  @Test
  public void testHomedirKerberos() throws Throwable {
    def home = RegistryUtils.homePathForUser("hbase@HADOOP.APACHE.ORG")
    try {
      Assert.assertEquals("/users/hbase", home)
    } catch (AssertionError e) {
      SliderTestUtils.skip("homedir filtering not yet in hadoop registry module")
    }
  }
    
  @Test
  public void testHomedirKerberosHost() throws Throwable {
    def home = RegistryUtils.homePathForUser("hbase/localhost@HADOOP.APACHE.ORG")
    try {
      Assert.assertEquals("/users/hbase", home)
    } catch (AssertionError e) {
      SliderTestUtils.skip("homedir filtering not yet in hadoop registry module")
    }
  }

  @Test
  public void testRegistryPathForInstance() throws Throwable {
    def path = SliderRegistryUtils.registryPathForInstance("instance")
    assert path.endsWith("/instance")
  }

  @Test
  public void testPathResolution() throws Throwable {
    def home = RegistryUtils.homePathForCurrentUser()
    assert home == SliderRegistryUtils.resolvePath("~")
    assert (home +"/") == SliderRegistryUtils.resolvePath("~/")
    assert (home +"/something") == SliderRegistryUtils.resolvePath("~/something")
    assert ("~unresolved") == SliderRegistryUtils.resolvePath("~unresolved")
  }
  
}
