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

package org.apache.slider.funtest.lifecycle

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

@CompileStatic
@Slf4j
public class ClusterBuildDestroyIT extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String CLUSTER = "test-cluster-build-destroy"
  

  @BeforeClass
  public static void prepareCluster() {
    
    setupCluster(CLUSTER)
  }

  @AfterClass
  public static void destroyCluster() {
    teardown(CLUSTER)
  }
  
  @Test
  public void testBuildAndDestroyCluster() throws Throwable {
    def clusterDir = SliderKeys.SLIDER_BASE_DIRECTORY + "/cluster/$CLUSTER"
    def clusterDirPath = new Path(clusterFS.homeDirectory, clusterDir)
    clusterFS.delete(clusterDirPath, true)
    slider(EXIT_SUCCESS,
        [
            ACTION_BUILD,
            CLUSTER,
            ARG_ZKHOSTS,
            SLIDER_CONFIG.get(
                RegistryConstants.KEY_REGISTRY_ZK_QUORUM, DEFAULT_SLIDER_ZK_HOSTS),
            ARG_TEMPLATE, APP_TEMPLATE,
            ARG_RESOURCES, APP_RESOURCE
        ])

    assert clusterFS.exists(clusterDirPath)
    //cluster exists if you don't want it to be live
    exists(EXIT_SUCCESS, CLUSTER, false)
    // condition returns false if it is required to be live
    exists(EXIT_FALSE, CLUSTER, true)
    destroy(CLUSTER)

  }


}
