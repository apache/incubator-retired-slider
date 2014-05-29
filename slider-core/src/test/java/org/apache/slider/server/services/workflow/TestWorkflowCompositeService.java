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
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TestWorkflowCompositeService extends Assert {
  private static final Logger
      log = LoggerFactory.getLogger(TestWorkflowCompositeService.class);

  @Test
  public void testSingleCompound() throws Throwable {
    WorkflowCompositeService parent = startService(new MockService());
    parent.stop();
  }

  @Test
  public void testSingleCompoundTerminating() throws Throwable {
    WorkflowCompositeService parent =
        startService(new MockService("1", false, 100));
    assertTrue(waitForParentToStop(parent));
  }


  @Test
  public void testSingleCompoundFailing() throws Throwable {
    WorkflowCompositeService parent =
        startService(new MockService("1", true, 100));
    assert parent.waitForServiceToStop(1000);
    assert parent.getFailureCause() != null;
  }

  @Test
  public void testCompound() throws Throwable {
    MockService one = new MockService("one", false, 100);
    MockService two = new MockService("two", false, 100);
    WorkflowCompositeService parent = startService(one, two);
    assert waitForParentToStop(parent);
    assert one.isInState(Service.STATE.STOPPED);
    assert two.isInState(Service.STATE.STOPPED);
  }

  @Test
  public void testCompoundOneLongLived() throws Throwable {
    MockService one = new MockService("one", false, 500);
    MockService two = new MockService("two", false, 100);
    WorkflowCompositeService parent = startService(one, two);
    assert waitForParentToStop(parent);
    assert one.isInState(Service.STATE.STOPPED);
    assert two.isInState(Service.STATE.STOPPED);
  }

  boolean notified = false;
  
  @Test
  public void testNotificationInCompound() throws Throwable {

    WorkflowEventCallback ecb = new WorkflowEventCallback() {
      @Override
      public void eventCallbackEvent() {
        log.info("EventCallback");
        notified = true;
      }
    };
    MockService one = new MockService("one", false, 100);
    WorkflowEventNotifyingService ens =
        new WorkflowEventNotifyingService(ecb, 100);
    MockService two = new MockService("two", false, 100);
    WorkflowCompositeService parent = startService(one, ens, two);
    assertTrue(waitForParentToStop(parent));
    assertInState(one, Service.STATE.STOPPED);
    assertInState(ens, Service.STATE.STOPPED);
    assertInState(two, Service.STATE.STOPPED);
    assertTrue(notified);
  }
  
  public void assertInState(Service service, Service.STATE expected) {
    Service.STATE actual = service.getServiceState();
    if (actual != expected) {
      fail("Service " + service.getName() + " in state " + actual
       + " -expected " + expected);
    }
  }


  @Test
  public void testCompoundInCompound() throws Throwable {
    MockService one = new MockService("one", false, 100);
    MockService two = new MockService("two", false, 100);
    WorkflowCompositeService parent = buildService(one, two);
    WorkflowCompositeService outer = startService(parent);
    assertTrue(outer.waitForServiceToStop(1000));
    assertInState(one, Service.STATE.STOPPED);
    assertInState(two, Service.STATE.STOPPED);
  }
  
  @Test
  public void testFailingCompound() throws Throwable {
    MockService one = new MockService("one", true, 100);
    MockService two = new MockService("two", false, 100);
    WorkflowCompositeService parent = startService(one, two);
    assertTrue(waitForParentToStop(parent));
    assertInState(one, Service.STATE.STOPPED);
    assertInState(two, Service.STATE.STOPPED);
    assertNotNull(one.getFailureCause());
    assertEquals(one.getFailureCause(), parent.getFailureCause());
  }


  public boolean waitForParentToStop(WorkflowCompositeService parent) {
    boolean stop = parent.waitForServiceToStop(1000);
    if (!stop) {
      log.error("Service failed to stop {}", parent);
      logState(parent);
    }
    return stop;
  }

  public WorkflowCompositeService buildService(List<Service> services) {
    WorkflowCompositeService parent = new WorkflowCompositeService("test", services);
    parent.init(new Configuration());
    return parent;
  }
    public WorkflowCompositeService buildService(Service... services) {
    WorkflowCompositeService parent = new WorkflowCompositeService("test", services);
    parent.init(new Configuration());
    return parent;
  }
  
  public WorkflowCompositeService startService(List<Service> services) {
    WorkflowCompositeService parent = buildService(services);
    //expect service to start and stay started
    parent.start();
    return parent;
  }
  
  public WorkflowCompositeService startService(Service ...services) {
    WorkflowCompositeService parent = buildService(services);
    //expect service to start and stay started
    parent.start();
    return parent;
  }

  void logState(ServiceParent p) {
    logService(p);
    for (Service s : p.getServices()) {
      logService(s);
    }
  }

  public void logService(Service s) {
    log.info(s.toString());
    Throwable failureCause = s.getFailureCause();
    if (failureCause != null) {
      log.info("Failed in state {} with {}", s.getFailureState(),
          failureCause);
    }
  }
}
