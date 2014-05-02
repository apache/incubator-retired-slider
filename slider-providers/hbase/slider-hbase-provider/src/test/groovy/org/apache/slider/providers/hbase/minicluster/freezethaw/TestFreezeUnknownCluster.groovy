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

package org.apache.slider.providers.hbase.minicluster.freezethaw

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.common.params.SliderActions
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestFreezeUnknownCluster extends HBaseMiniClusterTestBase {

  @Test
  public void testFreezeUnknownCluster() throws Throwable {
    String clustername = "test_start_unknown_cluster"
    YarnConfiguration conf = getConfiguration()
    createMiniCluster(clustername, conf, 1, true)

    describe "try to freeze a cluster that isn't defined"

    //we are secretly picking up the RM details from the configuration file
    try {
      ServiceLauncher command = execSliderCommand(conf,
                                                [
                                                    SliderActions.ACTION_FREEZE,
                                                    "no-such-cluster"
                                                ]);
      fail("Expected an error, got an exit code of ${command.serviceExitCode}")
    } catch (UnknownApplicationInstanceException e) {
      //expected
    }
  }


}
