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

import groovy.util.logging.Slf4j
import org.apache.slider.api.ClusterDescription
import org.apache.slider.providers.accumulo.AccumuloKeys
import org.apache.slider.providers.accumulo.AccumuloTestBase
import org.junit.Test

@Slf4j
class TestAccFlexTablets133Mgr113 extends AccumuloTestBase {

  @Test
  public void testAccFlexTablets133Mgr113() throws Throwable {
    ClusterDescription cd = flexAccClusterTestRun(
        "test_acc_flex_tablets133mgr113",
        [
            [
                (AccumuloKeys.ROLE_MASTER) : 1,
                (AccumuloKeys.ROLE_TABLET) : 1,
                (AccumuloKeys.ROLE_MONITOR): 1,
                (AccumuloKeys.ROLE_GARBAGE_COLLECTOR): 1
            ],
            [
                (AccumuloKeys.ROLE_MASTER) : 1,
                (AccumuloKeys.ROLE_TABLET) : 3,
                (AccumuloKeys.ROLE_MONITOR): 1,
                (AccumuloKeys.ROLE_GARBAGE_COLLECTOR): 1
            ],
            [
                (AccumuloKeys.ROLE_MASTER) : 3,
                (AccumuloKeys.ROLE_TABLET) : 3,
                (AccumuloKeys.ROLE_MONITOR): 1,
                (AccumuloKeys.ROLE_GARBAGE_COLLECTOR): 1
            ],

        ]
    )
    
    log.info("Final CD \n{}", cd)
  }

}
