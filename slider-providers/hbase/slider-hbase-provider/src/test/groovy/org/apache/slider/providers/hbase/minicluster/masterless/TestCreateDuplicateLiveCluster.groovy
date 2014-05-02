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
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestCreateDuplicateLiveCluster extends HBaseMiniClusterTestBase {

    @Test
    public void testCreateClusterRunning() throws Throwable {
      String clustername = "test_create_duplicate_live_cluster"
      createMiniCluster(clustername, getConfiguration(), 1, true)

      describe "create a masterless AM, while it is running, try to create" +
               "a second cluster with the same name"

      //launch fake master
      ServiceLauncher launcher
      launcher = createMasterlessAM(clustername, 0, true, true)
      SliderClient sliderClient = (SliderClient) launcher.service
      addToTeardown(sliderClient);

      //now try to create instance #2, and expect an in-use failure
    try {
      createMasterlessAM(clustername, 0, false, true)
      fail("expected a failure")
    } catch (SliderException e) {
      assertFailureClusterInUse(e);
    }


  }


}
