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

package org.apache.slider.test

import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.StatusKeys
import org.junit.Test

/**
 * Test for some of the command test base operations
 */
class TestAssertions {

  public static final String CLUSTER_JSON = "json/cluster.json"

  @Test
  public void testNoInstances() throws Throwable {
    ClusterDescription clusterDescription = new ClusterDescription();
    clusterDescription.instances = null
    SliderTestUtils.assertContainersLive(clusterDescription, "example", 0);
  }

  @Test
  public void testEmptyInstances() throws Throwable {
    ClusterDescription clusterDescription = new ClusterDescription();
    SliderTestUtils.assertContainersLive(clusterDescription, "example", 0);
  }

  @Test
  public void testLiveInstances() throws Throwable {
    def stream = getClass().getClassLoader().getResourceAsStream(CLUSTER_JSON)
    assert stream != null, "could not load $CLUSTER_JSON"
    ClusterDescription liveCD = ClusterDescription.fromStream(stream)
    assert liveCD != null
    SliderTestUtils.assertContainersLive(liveCD, "SLEEP_LONG", 4)
    assert 1 == liveCD.statistics["SLEEP_LONG"][StatusKeys.STATISTICS_CONTAINERS_ANTI_AFFINE_PENDING]
  }

}
