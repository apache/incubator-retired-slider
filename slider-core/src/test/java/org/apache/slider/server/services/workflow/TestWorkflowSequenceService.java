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

package org.apache.slider.server.services.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWorkflowSequenceService extends WorkflowServiceTestBase {
  private static final Logger
      log = LoggerFactory.getLogger(TestWorkflowSequenceService.class);


  @Test
  public void testSingleSequence() throws Throwable {
    ServiceParent ss = startService(new MockService());
    ss.stop();
  }

  @Test
  public void testSequence() throws Throwable {
    MockService one = new MockService("one", false, 100);
    MockService two = new MockService("two", false, 100);
    ServiceParent ss = startService(one, two);;
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED);
    assert two.isInState(Service.STATE.STOPPED);
    assert ((WorkflowSequenceService)ss).getPreviousService().equals(two);
  }

  @Test
  public void testNotificationInSequence() throws Throwable {
    EventCallbackHandler ecb = new EventCallbackHandler();
    MockService one = new MockService("one", false, 100);
    WorkflowEventNotifyingService ens =
        new WorkflowEventNotifyingService(ecb, 100);
    MockService two = new MockService("two", false, 100);
    ServiceParent ss = startService(one, ens, two);
    assert ss.waitForServiceToStop(1000);
    assertStopped(one);
    assertStopped(ens);
    assertStopped(two);
    assertTrue(ecb.notified);
  }

  @Test
  public void testFailingSequence() throws Throwable {
    MockService one = new MockService("one", true, 100);
    MockService two = new MockService("two", false, 100);
    WorkflowSequenceService parent =
        (WorkflowSequenceService) startService(one, two);
    waitForParentToStop(parent);
    assertStopped(one);
    assertInState(two, Service.STATE.NOTINITED);
    assertEquals(one, parent.getPreviousService());
  }


  @Test
  public void testFailInStartNext() throws Throwable {
    MockService one = new MockService("one", false, 100);
    MockService two = new MockService("two", true, 0);
    MockService three = new MockService("3", false, 0);
    ServiceParent parent = startService(one, two, three);
    waitForParentToStop(parent);
    assertStopped(one);
    assertStopped(two);
    Throwable failureCause = two.getFailureCause();
    assertNotNull(failureCause);
    Throwable parentFailureCause = parent.getFailureCause();
    assertNotNull(parentFailureCause);
    assertEquals(parentFailureCause, failureCause);
    assertInState(three, Service.STATE.NOTINITED);
  }

  @Test
  public void testSequenceInSequence() throws Throwable {
    MockService one = new MockService("one", false, 100);
    MockService two = new MockService("two", false, 100);
    ServiceParent parent = buildService(one, two);
    ServiceParent outer = startService(parent);
    waitForParentToStop(parent);
    assertStopped(one);
    assertStopped(two);
  }

  @Test
  public void testVarargsConstructor() throws Throwable {
    MockService one = new MockService("one", false, 100);
    MockService two = new MockService("two", false, 100);
    ServiceParent parent = new WorkflowSequenceService("test", one, two);
    parent.init(new Configuration());
    parent.start();
    waitForParentToStop(parent);
    assertStopped(one);
    assertStopped(two);
  }

  public WorkflowSequenceService buildService(Service... services) {
    WorkflowSequenceService parent =
        new WorkflowSequenceService("test", services);
    parent.init(new Configuration());
    return parent;
  }

}
