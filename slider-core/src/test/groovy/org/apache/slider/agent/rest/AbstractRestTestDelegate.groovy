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

package org.apache.slider.agent.rest

import org.apache.slider.test.SliderTestUtils

/**
 * Base class for anything we want common to all
 */
abstract class AbstractRestTestDelegate extends SliderTestUtils {
  public static final String TEST_GLOBAL_OPTION = "test.global.option"
  public static final String TEST_GLOBAL_OPTION_PRESENT = "present"
  public static final int STOP_WAIT_TIME = 30000
  public static final int STOP_PROBE_INTERVAL = 500
  public final boolean enableComplexVerbs

  AbstractRestTestDelegate(boolean enableComplexVerbs) {
    this.enableComplexVerbs = enableComplexVerbs
  }


  public abstract void testSuiteGetOperations()

  public abstract void testSuiteComplexVerbs()

  public void testSuiteAll() {
    testSuiteGetOperations()
    if (enableComplexVerbs) {
      testSuiteComplexVerbs()
    }
  }
}
