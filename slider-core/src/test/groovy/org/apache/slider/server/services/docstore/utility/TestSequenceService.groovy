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
class TestSequenceService extends ServiceLauncherBaseTest {


  @Test
  public void testSingleSequence() throws Throwable {
    SequenceService ss = startService([new MockService()])
    ss.stop();
  }

  @Test
  public void testSequence() throws Throwable {
    MockService one = new MockService("one", false, 100)
    MockService two = new MockService("two", false, 100)
    SequenceService ss = startService([one, two])
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
    assert ss.previousService == two
  }

  @Test
  public void testNotificationInSequence() throws Throwable {
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
    SequenceService ss = startService([one, ens, two])
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert ens.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
    assert notified
  }

  @Test
  public void testFailingSequence() throws Throwable {
    MockService one = new MockService("one", true, 100)
    MockService two = new MockService("two", false, 100)
    SequenceService ss = startService([one, two])
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.NOTINITED)
    assert ss.previousService == one

  }
  


  @Test
  public void testFailInStartNext() throws Throwable {
    MockService one = new MockService("one", false, 100)
    MockService two = new MockService("two", true, 0)
    MockService three = new MockService("3", false, 0)
    SequenceService ss = startService([one, two, three])
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
    Throwable failureCause = two.failureCause
    assert failureCause != null;
    Throwable masterFailureCause = ss.failureCause
    assert masterFailureCause != null;
    assert masterFailureCause == failureCause

    assert three.isInState(Service.STATE.NOTINITED)
  }

  @Test
  public void testSequenceInSequence() throws Throwable {
    MockService one = new MockService("one", false, 100)
    MockService two = new MockService("two", false, 100)
    SequenceService ss = buildService([one, two])
    SequenceService outer = startService([ss])
    
    assert outer.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)
  }


  @Test
  public void testVarargsCtor() throws Throwable {
    MockService one = new MockService("one", false, 100)
    MockService two = new MockService("two", false, 100)
    SequenceService ss = new SequenceService("test", one, two);
    ss.init(new Configuration())
    ss.start();
    assert ss.waitForServiceToStop(1000);
    assert one.isInState(Service.STATE.STOPPED)
    assert two.isInState(Service.STATE.STOPPED)


  }
  public SequenceService startService(List<Service> services) {
    SequenceService ss = buildService(services)
    //expect service to start and stay started
    ss.start();
    return ss
  }

  public SequenceService buildService(List<Service> services) {
    SequenceService ss = new SequenceService("test")
    services.each { ss.addService(it) }
    ss.init(new Configuration())
    return ss
  }


}
