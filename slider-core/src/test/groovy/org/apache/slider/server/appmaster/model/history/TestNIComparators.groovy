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
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.state.NodeInstance
import org.junit.Test

/**
 * Unit test to verify the comparators sort as expected
 */
class TestNIComparators extends BaseMockAppStateTest  {

  NodeInstance age1Active4 = nodeInstance(1000, 4, 0, 0)
  NodeInstance age2Active2 = nodeInstance(1001, 2, 0, 0)
  NodeInstance age3Active0 = nodeInstance(1002, 0, 0, 0)
  NodeInstance age4Active1 = nodeInstance(1005, 0, 0, 0)
  NodeInstance empty = new NodeInstance("empty", MockFactory.ROLE_COUNT)

  List<NodeInstance> nodes = [age2Active2, age4Active1, age1Active4, age3Active0]

  @Override
  String getTestName() {
    return "TestNIComparators"
  }

  @Test
  public void testNewerThan() throws Throwable {

    Collections.sort(nodes, new NodeInstance.newerThan(0))
    assertListEquals(nodes,
                     [age4Active1, age3Active0, age2Active2, age1Active4])
  }

  @Test
  public void testNewerThanNoRole() throws Throwable {

    nodes << empty
    Collections.sort(nodes, new NodeInstance.newerThan(0))
    assertListEquals(nodes,
                     [age4Active1, age3Active0, age2Active2, age1Active4, empty])
  }

  @Test
  public void testMoreActiveThan() throws Throwable {

    Collections.sort(nodes, new NodeInstance.moreActiveThan(0))
    assertListEquals(nodes,
                     [age1Active4, age2Active2, age4Active1, age3Active0],)
  }

  @Test
  public void testMoreActiveThanEmpty() throws Throwable {
    nodes << empty
    Collections.sort(nodes, new NodeInstance.moreActiveThan(0))
    assertListEquals(nodes,
                     [age1Active4, age2Active2, age4Active1, age3Active0, empty])
  }

}
