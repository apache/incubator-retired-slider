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

package org.apache.slider.funtest.basic

import org.junit.Test

import static org.apache.slider.funtest.framework.SliderShell.signCorrect

/**
 * This just verifies the two's complement sign correction that will
 * be applied after the return code is picked up from the shell
 */
class SignCorrectionIT {

  @Test
  public void test255ToMinus1() throws Throwable {
    assert -1 == signCorrect(255) 
  }
  @Test
  public void test74To74() throws Throwable {
    assert 74 == signCorrect(74) 
  }
  @Test
  public void test1To1() throws Throwable {
    assert 1 == signCorrect(1) 
  }
}
