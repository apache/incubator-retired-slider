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

package org.apache.slider.server.appmaster.model.appstate

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.state.AppStateBindingInfo
import org.apache.slider.server.appmaster.state.RoleStatus

/**
 * class for basis of Anti-affine placement tests; sets up role2
 * for anti-affinity
 */
@CompileStatic
@Slf4j
class BaseMockAppStateAATest extends BaseMockAppStateTest
    implements MockRoles {

  /** Role status for the base AA role */
  RoleStatus aaRole

  /** Role status for the AA role requiring a node with the gpu label */
  RoleStatus gpuRole

  @Override
  AppStateBindingInfo buildBindingInfo() {
    def bindingInfo = super.buildBindingInfo()
    bindingInfo.roles = [
        MockFactory.PROVIDER_ROLE0,
        MockFactory.AAROLE_1_GPU,
        MockFactory.AAROLE_2,
    ]
    bindingInfo
  }

  @Override
  void setup() {
    super.setup()
    aaRole = lookupRole(MockFactory.AAROLE_2.name)
    gpuRole = lookupRole(MockFactory.AAROLE_1_GPU.name)
  }

}
