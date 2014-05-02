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

package org.apache.slider.providers.hbase.funtest

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.providers.hbase.HBaseKeys
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

@CompileStatic
@Slf4j
public class TestClusterBuildDestroy extends HBaseCommandTestBase
    implements FuntestProperties, Arguments {


  static String CLUSTER = "test_cluster_build_destroy"
  

  @BeforeClass
  public static void prepareCluster() {
    assumeFunctionalTestsEnabled();
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
    slider(0,
        [
            SliderActions.ACTION_BUILD,
            CLUSTER,
            ARG_PROVIDER, HBaseKeys.PROVIDER_HBASE,
            ARG_ZKHOSTS,
            SLIDER_CONFIG.get(SliderXmlConfKeys.REGISTRY_ZK_QUORUM, DEFAULT_SLIDER_ZK_HOSTS),
            ARG_IMAGE,
            SLIDER_CONFIG.get(KEY_TEST_HBASE_TAR),
            ARG_CONFDIR,
            SLIDER_CONFIG.get(KEY_TEST_HBASE_APPCONF),
            ARG_COMPONENT, HBaseKeys.ROLE_MASTER, "1",
            ARG_COMPONENT, HBaseKeys.ROLE_WORKER, "1",
            ARG_OPTION, "site.hbase.master.info.port", "8180",
        ])



    assert clusterFS.exists(clusterDirPath)
    //cluster exists if you don't want it to be live
    exists(0, CLUSTER, false)
    // condition returns false if it is required to be live
    exists(LauncherExitCodes.EXIT_FALSE, CLUSTER, true)
    destroy(CLUSTER)

  }


}
