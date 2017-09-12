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
package org.apache.slider.server.services.security;

import org.junit.Assert;
import org.junit.Test;

public class TestSecurityUtils {

  @Test
  public void testRandomAlphanumeric() throws Exception {
    int passLength = 50;
    String password = SecurityUtils.randomAlphanumeric(passLength);
    Assert.assertEquals(
        "Returned string length does not match requested length", passLength,
        password.length());

    // 0 length
    password = SecurityUtils.randomAlphanumeric(0);
    Assert.assertTrue("Returned string should be empty", password.isEmpty());
  }

  @Test(expected = NegativeArraySizeException.class)
  public void testRandomAlphanumericException() throws Exception {
    SecurityUtils.randomAlphanumeric(-1);
  }
}
