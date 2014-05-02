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

package org.apache.slider.client

import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.main.ServiceLauncherBaseTest
import org.junit.Test

/**
 * Test bad argument handling
 */
//@CompileStatic
class TestClientBasicArgs extends ServiceLauncherBaseTest {

  /**
   * help should print out help string and then succeed
   * @throws Throwable
   */
  @Test
  public void testHelp() throws Throwable {
    ServiceLauncher launcher = launch(SliderClient,
                                      SliderUtils.createConfiguration(),
                                      [ClientArgs.ACTION_HELP])
    assert 0 == launcher.serviceExitCode
  } 
  
  @Test
  public void testNoArgs() throws Throwable {
    try {
      ServiceLauncher launcher = launch(SliderClient,
                                        SliderUtils.createConfiguration(),
                                        [])
      assert SliderExitCodes.EXIT_COMMAND_ARGUMENT_ERROR == launcher.serviceExitCode
    } catch (BadCommandArgumentsException ignored) {
      // expected
    }
  }

  @Test
  public void testListUnknownHost() throws Throwable {
    try {
      ServiceLauncher launcher = launch(SliderClient,
                                        SliderUtils.createConfiguration(),
                                        [
                                        ClientArgs.ACTION_LIST,
                                        "cluster",
                                        Arguments.ARG_MANAGER,
                                        "unknownhost.example.org:80"])
      fail("expected an exception, got a launcher with exit code $launcher.serviceExitCode")
    } catch (UnknownHostException expected) {
      //expected
    }

  }

}
