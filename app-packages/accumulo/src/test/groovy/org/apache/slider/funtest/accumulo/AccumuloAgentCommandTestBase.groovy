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
package org.apache.slider.funtest.accumulo

import groovy.util.logging.Slf4j
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.junit.After
import org.junit.Before

@Slf4j
abstract class AccumuloAgentCommandTestBase extends AgentCommandTestBase {
  protected static final int ACCUMULO_LAUNCH_WAIT_TIME
  protected static final int ACCUMULO_GO_LIVE_TIME

  protected static final String USER = "root"
  protected static final String PASSWORD = "secret_password"
  protected static final String INSTANCE_SECRET = "other_secret_password"

  static {
    ACCUMULO_LAUNCH_WAIT_TIME = getTimeOptionMillis(SLIDER_CONFIG,
      KEY_ACCUMULO_LAUNCH_TIME,
      1000 * DEFAULT_ACCUMULO_LAUNCH_TIME_SECONDS)
    ACCUMULO_GO_LIVE_TIME = getTimeOptionMillis(SLIDER_CONFIG,
      KEY_ACCUMULO_GO_LIVE_TIME,
      1000 * DEFAULT_ACCUMULO_LIVE_TIME_SECONDS)
  }

  abstract public String getClusterName();

  @Before
  public void prepareCluster() {
    setupCluster(getClusterName())
  }

  @After
  public void destroyCluster() {
    cleanup(getClusterName())
  }
}
