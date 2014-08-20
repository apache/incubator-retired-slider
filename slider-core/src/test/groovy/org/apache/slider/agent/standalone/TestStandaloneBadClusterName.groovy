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

package org.apache.slider.agent.standalone

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLaunchException
import org.junit.Test

@CompileStatic
@Slf4j

class TestStandaloneBadClusterName extends AgentMiniClusterTestBase {

  @Test
  public void testStandaloneBadClusterName() throws Throwable {
    String clustername = "TestStandaloneBadClusterName"
    createMiniCluster(clustername, configuration, 1, true)

    describe "verify that bad cluster names are picked up"

    try {
      addToTeardown(createStandaloneAM(clustername, true, false).service);
      fail("expected a failure")
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR)
    }
    
  }

}
