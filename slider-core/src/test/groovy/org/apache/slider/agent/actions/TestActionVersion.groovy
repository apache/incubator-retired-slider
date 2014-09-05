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

package org.apache.slider.agent.actions

import groovy.util.logging.Slf4j
import org.apache.slider.common.params.SliderActions
import org.apache.slider.test.SliderTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Slf4j

class TestActionVersion extends SliderTestBase {

  @Test
  public void testVersion() throws Throwable {
    
    ServiceLauncher launcher = execSliderCommand(
        new YarnConfiguration(),
        [
            SliderActions.ACTION_VERSION,
        ]
    )
    assert launcher.serviceExitCode == 0
  }

}
