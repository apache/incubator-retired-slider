/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.providers.hbase.minicluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.common.SliderKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

import static HBaseKeys.PROVIDER_HBASE
import static Arguments.ARG_PROVIDER

@CompileStatic
@Slf4j

class TestRoleOptPropagation extends HBaseMiniClusterTestBase {

  @Test
  
  public void testRoleOptPropagation() throws Throwable {
    skip("Disabled")
    String clustername = "test_role_opt_propagation"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that role options propagate down to deployed roles"

    String ENV = "env.ENV_VAR"
    ServiceLauncher launcher = createCluster(clustername,
                   [
                       (HBaseKeys.ROLE_MASTER): 0,
                       (HBaseKeys.ROLE_WORKER): 0,
                   ],
                   [
                       Arguments.ARG_COMP_OPT, HBaseKeys.ROLE_MASTER, ENV, "4",
                       ARG_PROVIDER, PROVIDER_HBASE
                   ],
                   true,
                   true,
                   [:])
    SliderClient sliderClient = (SliderClient) launcher.service
    addToTeardown(sliderClient);
    ClusterDescription status = sliderClient.clusterDescription
    Map<String, String> masterRole = status.getRole(HBaseKeys.ROLE_MASTER);
    dumpClusterDescription("Remote CD", status)
    assert masterRole[ENV] == "4"

  }

  @Test
  public void testUnknownRole() throws Throwable {
    String clustername = "test_unknown_role"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that unknown role results in cluster creation failure"
    try {
      String MALLOC_ARENA = "env.MALLOC_ARENA_MAX"
      ServiceLauncher launcher = createCluster(clustername,
         [
             (HBaseKeys.ROLE_MASTER): 0,
             (HBaseKeys.ROLE_WORKER): 0,
         ],
         [
             Arguments.ARG_COMP_OPT, SliderKeys.COMPONENT_AM, MALLOC_ARENA, "4",
             Arguments.ARG_COMP_OPT, "unknown", MALLOC_ARENA, "3",
             ARG_PROVIDER, PROVIDER_HBASE
         ],
         true,
         true,
         [:])
      assert false
    } catch (BadCommandArgumentsException bcae) {
      /* expected */
    }
  }
}
