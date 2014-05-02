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

package org.apache.slider.server.appmaster.model.history

import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.OutstandingRequest
import org.apache.slider.server.appmaster.state.OutstandingRequestTracker
import org.junit.Test

class TestOutstandingRequestTracker extends BaseMockAppStateTest {

  NodeInstance host1 = new NodeInstance("host1", 3)
  NodeInstance host2 = new NodeInstance("host2", 3)

  OutstandingRequestTracker tracker = new OutstandingRequestTracker()
  
  @Override
  String getTestName() {
    return "TestOutstandingRequestTracker"
  }

  @Test
  public void testAddRetrieveEntry() throws Throwable {
    OutstandingRequest request = tracker.addRequest(host1, 0)
    assert tracker.lookup(0, "host1").equals(request)
    assert tracker.remove(request).equals(request)
    assert !tracker.lookup(0, "host1")
  }

  @Test
  public void testAddCompleteEntry() throws Throwable {
    tracker.addRequest(host1, 0)
    tracker.addRequest(host2, 0)
    tracker.addRequest(host1, 1)
    assert tracker.onContainerAllocated(1, "host1")
    assert !tracker.lookup(1, "host1")
    assert tracker.lookup(0, "host1")
  }
  
  @Test
  public void testCancelEntries() throws Throwable {
    OutstandingRequest r1 = tracker.addRequest(host1, 0)
    OutstandingRequest r2 = tracker.addRequest(host2, 0)
    OutstandingRequest r3 = tracker.addRequest(host1, 1)
    List<NodeInstance> canceled = tracker.cancelOutstandingRequests(0)
    assert canceled.size() == 2
    assert canceled.contains(host1)
    assert canceled.contains(host2)
    assert tracker.lookup(1, "host1")
    assert !tracker.lookup(0, "host1")
    canceled = tracker.cancelOutstandingRequests(0)
    assert canceled.size() == 0
    assert tracker.cancelOutstandingRequests(1).size() == 1
  }
  
}
