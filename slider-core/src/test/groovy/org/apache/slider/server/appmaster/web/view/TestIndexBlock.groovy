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
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet
import org.apache.slider.api.SliderClusterProtocol
import org.apache.slider.providers.ProviderService
import org.apache.slider.server.appmaster.model.mock.*
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.web.WebAppApi
import org.apache.slider.server.appmaster.web.WebAppApiImpl
import org.junit.Before
import org.junit.Test

@Slf4j
@CompileStatic
public class TestIndexBlock {

  private IndexBlock indexBlock;

  private Container cont1, cont2;

  @Before
  public void setup() {
    SliderClusterProtocol clusterProto = new MockSliderClusterProtocol();
    AppState appState = new MockAppState(new MockRecordFactory());
    ProviderService providerService = new MockProviderService();

    WebAppApiImpl inst = new WebAppApiImpl(clusterProto, appState, providerService);

    Injector injector = Guice.createInjector(new AbstractModule() {
          @Override
          protected void configure() {
            bind(WebAppApi.class).toInstance(inst);
          }
        });

    indexBlock = injector.getInstance(IndexBlock.class);

    cont1 = new MockContainer();
    cont1.id = new MockContainerId();
    ((MockContainerId) cont1.id).setId(0);
    cont1.nodeId = new MockNodeId();
    cont1.priority = Priority.newInstance(1);
    cont1.resource = new MockResource();

    cont2 = new MockContainer();
    cont2.id = new MockContainerId();
    ((MockContainerId) cont2.id).setId(1);
    cont2.nodeId = new MockNodeId();
    cont2.priority = Priority.newInstance(1);
    cont2.resource = new MockResource();
  }

  @Test
  public void testIndex() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    
    int level = hamlet.nestLevel();
    indexBlock.doIndex(hamlet, "accumulo");
    
    assert level == hamlet.nestLevel();
  }
}