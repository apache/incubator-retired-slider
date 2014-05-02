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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.NodeMap
import org.junit.Before
import org.junit.Test

@Slf4j
@CompileStatic
class TestFindNodesForRelease extends BaseMockAppStateTest {


  @Override
  String getTestName() {
    return "TestFindNodesForRelease"
  }
  NodeInstance age1Active4 = nodeInstance(1, 4, 0, 0)
  NodeInstance age2Active2 = nodeInstance(2, 2, 0, 0)
  NodeInstance age3Active0 = nodeInstance(3, 0, 0, 0)
  NodeInstance age4Active1 = nodeInstance(4, 1, 0, 0)
  NodeInstance empty = new NodeInstance("empty", MockFactory.ROLE_COUNT)

  List<NodeInstance> nodes = [age2Active2, age4Active1, age1Active4, age3Active0]
  NodeMap nodeMap = new NodeMap(MockFactory.ROLE_COUNT);


  @Before
  public void setupNodeMap() {
    nodeMap.insert(nodes)
  }

  private void assertReleased(
      int count,
      List<NodeInstance> expected,
      int role = 0) {
    List<NodeInstance> released = nodeMap.findNodesForRelease(role, count)
    assertListEquals(released, expected)
  }
  private void assertReleased(
      List<NodeInstance> expected,
      int role = 0) {
    List<NodeInstance> released = nodeMap.findNodesForRelease(role, expected.size())
    assertListEquals(released, expected)
  }

  @Test
  public void testListActiveNodes() throws Throwable {
    assertListEquals(nodeMap.listActiveNodes(0),
                     [age1Active4,age2Active2, age4Active1])
  }
  
  @Test
  public void testReleaseMinus1() throws Throwable {
    try {
      nodeMap.findNodesForRelease(0, -1)
      fail("Expected an exception")
    } catch (IllegalArgumentException e) {
    }
  }  
  @Test
  public void testReleaseO() throws Throwable {
    assertReleased(0, [])
  }

  @Test
  public void testRelease1() throws Throwable {
    assertReleased(1, [age1Active4])
  }

  @Test
  public void testRelease2() throws Throwable {
    assertReleased(2, [age1Active4, age1Active4])
  }

  @Test
  public void testRelease3() throws Throwable {
    assertReleased(3, [age1Active4, age1Active4, age1Active4 ])
  }

  @Test
  public void testRelease4() throws Throwable {
    assertReleased(4, [age1Active4, age1Active4, age1Active4 , age2Active2])
  }

  @Test
  public void testRelease5() throws Throwable {
    assertReleased([age1Active4, age1Active4, age1Active4 , age2Active2, age4Active1])
  }

  @Test
  public void testRelease6() throws Throwable {
    assertReleased(
           [age1Active4, age1Active4, age1Active4 , age2Active2, age4Active1, age1Active4])
  }

  @Test
  public void testRelease7() throws Throwable {
    assertReleased(
           [age1Active4, age1Active4, age1Active4 , age2Active2, age4Active1,
               age1Active4, age2Active2])
  }

  @Test
  public void testRelease8() throws Throwable {
    assertReleased(8,
           [age1Active4, age1Active4, age1Active4 , age2Active2, age4Active1,
               age1Active4, age2Active2])
  }

  @Test
  public void testPurgeInactiveTime3() throws Throwable {
    assert nodeMap.purgeUnusedEntries(3) == 0;
  }

  @Test
  public void testPurgeInactiveTime4() throws Throwable {
    assert nodeMap.purgeUnusedEntries(4) == 1;
  }
  @Test
  public void testPurgeInactiveTime5() throws Throwable {
    assert nodeMap.purgeUnusedEntries(5) == 1;
  }

}
