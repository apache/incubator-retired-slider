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
package org.apache.slider.server.appmaster.web.view

import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.google.inject.Injector
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet
import org.apache.slider.api.ClusterDescription
import org.apache.slider.providers.ProviderService
import org.apache.slider.server.appmaster.model.mock.MockAppState
import org.apache.slider.server.appmaster.model.mock.MockProviderService
import org.apache.slider.server.appmaster.model.mock.MockRecordFactory
import org.apache.slider.server.appmaster.state.AbstractRecordFactory
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.state.ProviderAppState
import org.apache.slider.server.appmaster.web.WebAppApi
import org.apache.slider.server.appmaster.web.WebAppApiImpl
import org.junit.Before
import org.junit.Test

@Slf4j
//@CompileStatic
public class TestClusterSpecificationBlock {

  private ClusterSpecificationBlock clusterSpecBlock;

  @Before
  public void setup() {
    AppState appState = new MyAppState(new MockRecordFactory());
    ProviderAppState providerAppState = new ProviderAppState(
        "undefined",
        appState)
    ProviderService providerService = new MockProviderService();

    WebAppApiImpl inst = new WebAppApiImpl(
        providerAppState,
        providerService,
        null,
        null, null, null, null, null);

    Injector injector = Guice.createInjector(new AbstractModule() {
          @Override
          protected void configure() {
            bind(WebAppApi.class).toInstance(inst);
          }
        });

    clusterSpecBlock = injector.getInstance(ClusterSpecificationBlock.class);
  }

  @Test
  public void testJsonGeneration() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    
    int level = hamlet.nestLevel();
    clusterSpecBlock.doRender(hamlet);
    
    assert level == hamlet.nestLevel();
  }
  
  private static class MyAppState extends MockAppState {
    public MyAppState(AbstractRecordFactory recordFactory) {
      super(recordFactory);
      this.clusterStatus = new MockClusterDescription();
    }
  }
  
  private static class MockClusterDescription extends ClusterDescription {
    @Override
    public String toJsonString() {
      return "{\"foo\": \"bar\"}";
    }
  }

}