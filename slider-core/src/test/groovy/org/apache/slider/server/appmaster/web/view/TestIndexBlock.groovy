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
import org.apache.slider.server.appmaster.model.appstate.BaseMockAppStateAATest
import org.apache.slider.server.appmaster.model.mock.*
import org.apache.slider.server.appmaster.state.ContainerOutcome
import org.apache.slider.server.appmaster.state.OutstandingRequest
import org.apache.slider.server.appmaster.state.ProviderAppState
import org.apache.slider.server.appmaster.web.WebAppApi
import org.apache.slider.server.appmaster.web.WebAppApiImpl
import org.junit.Before
import org.junit.Test

@Slf4j
//@CompileStatic
public class TestIndexBlock extends BaseMockAppStateAATest {

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
    def role2 = role2Status

    def role0_desired = 8

    role0.desired = role0_desired
    int role0_actual = 5
    int role0_requested = role0_desired - role0_actual
    role0_actual.times {
      role0.incActual()
    }
    assert role0.getActual() == role0_actual
    role0_requested.times {
      role0.incRequested()
    }
    assert role0.getRequested() == role0_requested

    def role0_failures = 2

    role0.noteFailed(false, "", ContainerOutcome.Failed, null)
    role0.noteFailed(true,  "", ContainerOutcome.Failed, null)

    // all aa roles fields are in the
    def aarole_desired = 200
    aaRole.desired = aarole_desired
    def aarole_actual = 90
    def aarole_active = 1
    def aarole_requested = aarole_desired - aarole_actual
    def aarole_pending = aarole_requested - 1
    def aarole_failures = 0
    aarole_actual.times {
      aaRole.incActual()
    }
    assert aaRole.actual == aarole_actual
    aaRole.outstandingAArequest = new OutstandingRequest(2, "")
    // add a requested
    aaRole.incRequested()
    aaRole.setPendingAntiAffineRequests(aarole_pending)
    assert aaRole.pendingAntiAffineRequests == aarole_pending

    assert aaRole.actualAndRequested == aarole_actual + 1
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);

    indexBlock.doIndex(hamlet, "accumulo");

    def body = sw.toString()
    log.info(body)
    // verify role data came out
    assert body.contains("role0")
    assertContains(role0_desired, body)
    assertContains(role0_actual, body)
    assertContains(role0_requested, body)
    assertContains(role0_failures, body)

    assert body.contains("role1")
    assert body.contains("role2")

    assertContains(aarole_desired, body)
    assertContains(aarole_actual, body)
//    assertContains(aarole_requested, body)
    assertContains(aarole_failures, body)
    assert body.contains(indexBlock.buildAADetails(true, aarole_pending))

    // verify that the sorting took place
    assert body.indexOf("role0") < body.indexOf("role1")
    assert body.indexOf("role1") < body.indexOf("role2")

    assert !body.contains(IndexBlock.ALL_CONTAINERS_ALLOCATED)
    // role
  }

  def assertContains(int ex, String html) {
    assertStringContains(Integer.toString(ex), html)
  }
}