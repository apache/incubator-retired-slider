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

package org.apache.slider.common.tools

import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl
import org.apache.slider.core.launch.AMRestartSupport
import org.apache.slider.test.SliderTestBase
import org.junit.Test

@Slf4j
class TestSliderServiceUtils extends SliderTestBase {

  @Test
  public void testRetrieveContainers() throws Throwable {
    RegisterApplicationMasterResponsePBImpl registration = new RegisterApplicationMasterResponsePBImpl()

    def method = AMRestartSupport.retrieveContainersFromPreviousAttempt(
        registration)
    def hasMethod = method != null
    def containers = AMRestartSupport.retrieveContainersFromPreviousAttempt(
        registration)
    def success = containers != null;

    assert (hasMethod == success)
    log.info("AM container recovery support=$hasMethod")
  }
}
