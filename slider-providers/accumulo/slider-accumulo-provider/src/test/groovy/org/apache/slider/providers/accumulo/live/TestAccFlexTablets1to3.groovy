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

package org.apache.slider.providers.accumulo.live

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.api.ClusterDescription
import org.apache.slider.providers.accumulo.AccumuloKeys
import org.apache.slider.providers.accumulo.AccumuloTestBase
import org.junit.Test

@CompileStatic
@Slf4j
class TestAccFlexTablets1to3 extends AccumuloTestBase {

  @Test
  public void testAccFlexTablets1to3() throws Throwable {
    Map<String, Integer> plan1 = (Map<String, Integer>) [
        (AccumuloKeys.ROLE_MASTER): 1,
        (AccumuloKeys.ROLE_TABLET): 1,
        (AccumuloKeys.ROLE_MONITOR): 1,
        (AccumuloKeys.ROLE_GARBAGE_COLLECTOR): 1]

    Map<String, Integer> plan2 = (Map<String, Integer>) [
        (AccumuloKeys.ROLE_MASTER): 1,
        (AccumuloKeys.ROLE_TABLET): 3,
        (AccumuloKeys.ROLE_MONITOR): 1,
        (AccumuloKeys.ROLE_GARBAGE_COLLECTOR): 1]

    ClusterDescription cd = flexAccClusterTestRun(
        "test_acc_flex_tablets1to3",
        [plan1, plan2]
    )

    log.info("Final CD \n{}", cd)
  }




}
