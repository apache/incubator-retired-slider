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

package org.apache.slider.client

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.params.ActionDiagnosticArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.test.SliderTestBase
import org.apache.slider.test.YarnMiniClusterTestBase
import org.apache.slider.test.YarnZKMiniClusterTestBase
import org.junit.Test

@CompileStatic
@Slf4j
class TestDiagnostics extends YarnZKMiniClusterTestBase {

  @Test
  public void testClientDiags() throws Throwable {
    //launch fake master
    String clustername = createMiniCluster("", configuration, 1, true)
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [SliderActions.ACTION_DIAGNOSTICS,
         Arguments.ARG_CLIENT]
    )
    def client = launcher.service
    def diagnostics = new ActionDiagnosticArgs()
    diagnostics.client = true
    diagnostics.verbose = true
    describe("Verbose diagnostics")
    
    
    client.actionDiagnostic(diagnostics)

  }
}
