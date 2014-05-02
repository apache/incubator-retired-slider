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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.service.Service
import org.apache.hadoop.service.ServiceStateException
import org.junit.Test

class TestMockService {

  @Test
  public void testSimpleLifecycle() throws Throwable {
    MockService s = new MockService("1",false,-1);
    s.init(new Configuration())
    s.start();
    assert s.isInState(Service.STATE.STARTED)
  }
  
  @Test
  public void testSimpleLifecycleWait() throws Throwable {
    MockService s = new MockService("1",false,-1);
    s.init(new Configuration())
    s.start();
    assert s.isInState(Service.STATE.STARTED)
    s.stop();
    s.waitForServiceToStop(0);
  }
  
  @Test
  public void testStoppingService() throws Throwable {
    MockService s = new MockService("1",false,100);
    s.init(new Configuration())
    s.start();
    Thread.sleep(1000);
    assert s.isInState(Service.STATE.STOPPED)
  }
  
  @Test
  public void testStoppingWaitService() throws Throwable {
    MockService s = new MockService("1",false,100);
    s.init(new Configuration())
    s.start();
    s.waitForServiceToStop(0);
    assert s.isInState(Service.STATE.STOPPED)
  }
    
  
  
  @Test
  public void testFailingService() throws Throwable {
    MockService s = new MockService("1",true,100);
    s.init(new Configuration())
    s.start();
    s.waitForServiceToStop(0);

    assert s.isInState(Service.STATE.STOPPED)
    assert s.failureCause != null
  }
      
  @Test
  public void testFailingInStart() throws Throwable {
    MockService s = new MockService("1",true,0);
    s.init(new Configuration())
    try {
      s.start();
      //failure, raise a fault with some text
      assert null == s
    } catch (ServiceStateException e) {
      //expected
    }
    assert s.isInState(Service.STATE.STOPPED)
    assert s.failureCause != null
    s.waitForServiceToStop(0);
  }
  
  
}
