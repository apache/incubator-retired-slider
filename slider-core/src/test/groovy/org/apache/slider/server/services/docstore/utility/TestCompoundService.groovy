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

package org.apache.slider.server.services.docstore.utility

import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.service.Service
import org.apache.slider.core.main.ServiceLauncherBaseTest
import org.junit.Test

@Slf4j
class TestCompoundService extends ServiceLauncherBaseTest {


  @Test
  public void testSingleCompound() throws Throwable {
    CompoundService parent = startService([new MockService()])
    parent.stop();
  }
  
  
  @Test
  public void testSingleCompoundTerminating() throws Throwable {
    CompoundService parent = startService([new MockService("1",false,100)])
    assert waitForParentToStop(parent);
  }

  public boolean waitForParentToStop(CompoundService parent) {
    boolean stop = parent.waitForServiceToStop(1000)
    if (!stop) {
      log.error("Service failed to stop $parent")
      logState(parent)
    }
    return stop
  }


  @Test
  public void testSingleCompoundFailing() throws Throwable {
    CompoundService parent = startService([new MockService("1",true,100)])
    assert parent.waitForServiceToStop(1000);
    assert parent.getFailureCause() != null;
  }
  
  @Test
  public void testCompound() throws Throwable {
    MockService one = new MockService("one", false, 100)
    MockService two = new MockService("two", false, 100)
    CompoundService parent = startService([one, two])
    assert waitForParentToStop(parent);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
  }

  @Test
  public void testCompoundOneLongLived() throws Throwable {
    MockService one = new MockService("one", false, 500)
    MockService two = new MockService("two", false, 100)
    CompoundService parent = startService([one, two])
    assert waitForParentToStop(parent);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
  }

  
  @Test
  public void testNotificationInCompound() throws Throwable {
    boolean notified = false;
    EventCallback ecb = new EventCallback() {
      @Override
      void eventCallbackEvent() {
        log.info("EventCallback")
        notified = true;
      }
    }
    MockService one = new MockService("one", false, 100)
    EventNotifyingService ens = new EventNotifyingService(ecb, 100);
    MockService two = new MockService("two", false, 100)
    CompoundService parent = startService([one, ens, two])
    assert waitForParentToStop(parent);
    assert one.isInState(Service.STATE.STOPPED)
    assert ens.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
    assert notified
  }

  @Test
  public void testFailingCompound() throws Throwable {
    MockService one = new MockService("one", true, 100)
    MockService two = new MockService("two", false, 100)
    CompoundService parent = startService([one, two])
    assert waitForParentToStop(parent);



    assert one.isInState(Service.STATE.STOPPED)
    assert one.failureCause != null
    assert two.isInState(Service.STATE.STOPPED)
  }
  



  @Test
  public void testCompoundInCompound() throws Throwable {
    MockService one = new MockService("one", false, 100)
    MockService two = new MockService("two", false, 100)
    CompoundService parent = buildService([one, two])
    CompoundService outer = startService([parent])
        assert outer.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
  }

  public CompoundService startService(List<Service> services) {
    CompoundService parent = buildService(services)
    //expect service to start and stay started
    parent.start();
    return parent
  }

  public CompoundService buildService(List<Service> services) {
    CompoundService parent = new CompoundService("test")
    services.each { parent.addService(it) }
    parent.init(new Configuration())
    return parent
  }


  void logState(Parent p) {
    logService(p)
    for (Service s : p.services) {
      logService(s)
    }
  }

  public void logService(Service s) {
    log.info(s.toString())
    if (s.getFailureCause()) {
      log.info("Failed in state ${s.getFailureState()} with $s.failureCause")
    }
  }
}
