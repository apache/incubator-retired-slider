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

package org.apache.slider.server.appmaster.model.appstate

import groovy.util.logging.Slf4j
import org.apache.slider.api.types.SerializedContainerInformation
import org.apache.slider.core.persist.JsonSerDeser
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.state.ProviderAppState
import org.apache.slider.server.appmaster.state.RoleInstance
import org.apache.slider.server.appmaster.state.StateAccessForProviders
import org.apache.slider.server.appmaster.web.rest.application.resources.CachedContent
import org.apache.slider.server.appmaster.web.rest.application.resources.ContainerListRefresher
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache
import org.apache.slider.server.appmaster.web.rest.application.resources.ResourceRefresher
import org.junit.Test

@Slf4j
class TestMockAppStateAppRestIntegration extends BaseMockAppStateTest implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateAppRestIntegration"
  }

  @Test
  public void testCachedIntDocument() throws Throwable {
    ContentCache cache = new ContentCache()


    def refresher = new IntRefresher()
    assert 0 == refresher.count
    def entry = new CachedContentManagedTimer(refresher)
    cache.put("/int", entry)
    def content1 = cache.get("/int")
    assert entry.equals(content1)

    assert 0 == entry.get()
    assert 1 == refresher.count
    assert 0 == entry.cachedValue
    assert entry.refreshCounter == 1

    def got = entry.get()
    assert entry.refreshCounter == 2
    assert 1 == got;
  }

  @Test
  public void testContainerListRefresher() throws Throwable {
    int r0 = 1
    int r1 = 2
    int r2 = 3
    role0Status.desired = r0
    role1Status.desired = r1
    role2Status.desired = r2
    ContainerListRefresher clr = new ContainerListRefresher(stateAccess)
    def map = clr.refresh()
    assert map.size() == 0
    List<RoleInstance> instances = createAndStartNodes()
    map = clr.refresh()
    assert instances.size() == r0 + r1 + r2
    assert map.size() == instances.size()
    log.info("$map")
    JsonSerDeser<SerializedContainerInformation> serDeser =
        new JsonSerDeser<>(SerializedContainerInformation)
    map.each { key, value ->
      log.info("$key -> ${serDeser.toJson(value)}")
    }
  }

  public ProviderAppState getStateAccess() {
    StateAccessForProviders state = new ProviderAppState("name", appState)
    return state
  }

  /**
   * Little class to do integer refreshing & so test refresh logic
   */
  class IntRefresher implements ResourceRefresher<Integer>   {
    int count ;
    @Override
    Integer refresh() {
      log.info("Refresh at $count")
      def result = count
      count += 1;
      return result;
    }

    @Override
    String toString() {
      return "IntRefresher at " + count;
    }
    
  }

  class CachedContentManagedTimer extends CachedContent {
    int time = 0;
        
    @Override
    protected long now() {
      return time++;
    }

    CachedContentManagedTimer(ResourceRefresher refresh) {
      super(1, refresh)
    }

    @Override
    String toString() {
      return "CachedContentManagedTimer at " + time + "; " + super.toString();
    }
  }
  
  
}
