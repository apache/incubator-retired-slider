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
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet
import org.apache.slider.providers.ProviderService
import org.apache.slider.server.appmaster.model.mock.*
import org.apache.slider.server.appmaster.state.ContainerOutcome
import org.apache.slider.server.appmaster.state.ProviderAppState
import org.apache.slider.server.appmaster.web.WebAppApi
import org.apache.slider.server.appmaster.web.WebAppApiImpl
import org.junit.Before
import org.junit.Test

@Slf4j
//@CompileStatic
public class TestIndexBlock extends BaseMockAppStateTest {

  private IndexBlock indexBlock;

  private Container cont1, cont2;

  @Before
  public void setup() {
    super.setup()
    assert appState
    ProviderService providerService = new MockProviderService();
    ProviderAppState providerAppState = new ProviderAppState(
        "undefined",
        appState)

    WebAppApiImpl inst = new WebAppApiImpl(
        providerAppState,
        providerService,
        null,
        null, metrics, null, null, null);

    Injector injector = Guice.createInjector(new AbstractModule() {
          @Override
          protected void configure() {
            bind(WebAppApi.class).toInstance(inst);
          }
        });

    indexBlock = injector.getInstance(IndexBlock.class);

    cont1 = new MockContainer();
    cont1.id = new MockContainerId(applicationAttemptId, 0);
    cont1.nodeId = new MockNodeId();
    cont1.priority = Priority.newInstance(1);
    cont1.resource = new MockResource();

    cont2 = new MockContainer();
    cont2.id = new MockContainerId(applicationAttemptId, 1);
    cont2.nodeId = new MockNodeId();
    cont2.priority = Priority.newInstance(1);
    cont2.resource = new MockResource();
  }

  @Test
  public void testIndex() {
    def role0 = role0Status
    def role1 = role1Status
    role0.desired = 8
    role0.incActual()
    role0.incActual()
    role0.incActual()
    role0.incActual()
    role0.incActual()
    role1.incRequested()
    role1.incRequested()
    role1.incRequested()
    role0.noteFailed(false, "", ContainerOutcome.Failed)
    role0.noteFailed(true,  "", ContainerOutcome.Failed)

    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    
    int level = hamlet.nestLevel();
    indexBlock.doIndex(hamlet, "accumulo");

    def body = sw.toString()
    log.info(body)
    assertEquals(body, level, hamlet.nestLevel())
    // verify role data came out
    assert body.contains("role0")
    // 
    assert body.contains("8")
    assert body.contains("5")
    assert body.contains("3")
    assert body.contains("2")
    assert body.contains("1")
    
    assert body.contains("role1")
    assert body.contains("role2")
    // verify that the sorting took place
    assert body.indexOf("role0") < body.indexOf("role1")
    assert body.indexOf("role1") < body.indexOf("role2")
  }
}