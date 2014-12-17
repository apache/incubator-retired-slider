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
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.web.rest.application.resources.CachedContent
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache
import org.apache.slider.server.appmaster.web.rest.application.resources.ResourceRefresher
import org.junit.Test

@Slf4j
class TestMockAppStateAppResourceIntegration extends BaseMockAppStateTest implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateAppResourceIntegration"
  }

  @Test
  public void testCachedDocument() throws Throwable {
    ContentCache cache = new ContentCache()

    def content = new CachedContentManagedTimer(new IntRefresher())
    cache.put("/int", content)
    def content1 = cache.get("/int")
    assert content.equals(content1)
    
    assert 0 == content.get()
    assert 0 == content.getCachedValue()
    
  }

  class IntRefresher implements ResourceRefresher<Integer>   {
    int count ;
    @Override
    Integer refresh() {
      return count++;
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
    
  }
}
