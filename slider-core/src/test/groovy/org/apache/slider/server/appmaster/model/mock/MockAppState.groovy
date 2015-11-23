/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.appmaster.model.mock

import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring
import org.apache.slider.server.appmaster.state.AbstractClusterServices
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.state.AppStateBindingInfo

/**
 * Extended app state that makes more things public
 */
class MockAppState extends AppState {
  public static final int RM_MAX_RAM = 4096
  public static final int RM_MAX_CORES = 64

  public MockAppState(AbstractClusterServices recordFactory) {
    super(recordFactory, new MetricsAndMonitoring());
    setContainerLimits(1, RM_MAX_RAM, 1, RM_MAX_CORES)
  }

  long time = 0;
  
  /**
   * Instance with a mock record factory
   */
  public MockAppState() {
    this(new MockClusterServices());
  }

  MockAppState(AppStateBindingInfo bindingInfo) {
    this()
    buildInstance(bindingInfo)
  }

  public Map<String, ProviderRole> getRoleMap() {
    return super.roleMap;
  }

  /**
   * Current time. if the <code>time</code> field
   * is set, that value is returned
   * @return the current time.
   */
  protected long now() {
    return time ?: System.currentTimeMillis();
  }
}
