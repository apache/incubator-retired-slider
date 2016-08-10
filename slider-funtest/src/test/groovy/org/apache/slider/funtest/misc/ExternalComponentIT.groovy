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

package org.apache.slider.funtest.lifecycle

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ResourceKeys
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.ResourcePaths
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class ExternalComponentIT extends AgentCommandTestBase
  implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  static String NAME = "test-external-component"
  static String EXT_NAME = "test_sleep"
  static String NESTED_NAME = "test-external-component-nested"

  static String BUILD_APPCONFIG = ResourcePaths.SLEEP_APPCONFIG
  static String BUILD_RESOURCES = ResourcePaths.EXTERNAL_RESOURCES
  static String BUILD_METAINFO = ResourcePaths.SLEEP_META
  static String TEST_APPCONFIG = ResourcePaths.EXTERNAL_APPCONFIG
  static String TEST_RESOURCES = ResourcePaths.EXTERNAL_RESOURCES
  static String TEST_METAINFO = ResourcePaths.SLEEP_META
  static String NEST_APPCONFIG = ResourcePaths.NESTED_APPCONFIG
  static String NEST_RESOURCES = ResourcePaths.NESTED_RESOURCES
  static String NEST_METAINFO = ResourcePaths.NESTED_META
  public static final String SLEEP_100 = "SLEEP_100"
  public static final String SLEEP_LONG = "SLEEP_LONG"
  public static final String EXT_SLEEP_100 = EXT_NAME +
    SliderKeys.COMPONENT_SEPARATOR + SLEEP_100
  public static final String EXT_SLEEP_LONG = EXT_NAME +
    SliderKeys.COMPONENT_SEPARATOR + SLEEP_LONG
  public static final String NESTED_PREFIX = NAME +
    SliderKeys.COMPONENT_SEPARATOR

  @Before
  public void prepareCluster() {
    setupCluster(NAME)
    setupCluster(EXT_NAME)
    setupCluster(NESTED_NAME)
  }

  @After
  public void destroyCluster() {
    cleanup(NAME)
    cleanup(EXT_NAME)
    cleanup(NESTED_NAME)
  }

  @Test
  public void testExternalComponent() throws Throwable {
    assumeAgentTestsEnabled()
    describe NAME

    slider(0, [ACTION_BUILD, EXT_NAME, ARG_METAINFO, BUILD_METAINFO,
               ARG_TEMPLATE, BUILD_APPCONFIG, ARG_RESOURCES, BUILD_RESOURCES,
               ARG_RES_COMP_OPT, SLEEP_100, ResourceKeys.COMPONENT_INSTANCES, "0",
               ARG_RES_COMP_OPT, SLEEP_LONG, ResourceKeys.COMPONENT_INSTANCES, "1"
    ])

    slider(0, [ACTION_CREATE, NAME, ARG_METAINFO, TEST_METAINFO,
               ARG_TEMPLATE, TEST_APPCONFIG, ARG_RESOURCES, TEST_RESOURCES])

    ensureApplicationIsUp(NAME)
    status(0, NAME)

    ClusterDescription cd = execStatus(NAME)

    assert 5 == cd.statistics.size()
    assert cd.statistics.keySet().containsAll([SliderKeys.COMPONENT_AM,
                                               SLEEP_100, SLEEP_LONG,
                                               EXT_SLEEP_100, EXT_SLEEP_LONG])

    expectLiveContainerCountReached(NAME, SLEEP_LONG, 1,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NAME, EXT_SLEEP_LONG, 1,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NAME, SLEEP_100, 0,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NAME, EXT_SLEEP_100, 0,
      CONTAINER_LAUNCH_TIMEOUT)

    cleanup(NAME)

    // test overriding the number of instances of external components
    describe NAME + "-2"

    slider(0, [ACTION_CREATE, NAME, ARG_METAINFO, TEST_METAINFO,
               ARG_TEMPLATE, TEST_APPCONFIG, ARG_RESOURCES, TEST_RESOURCES,
               ARG_RES_COMP_OPT, SLEEP_LONG, ResourceKeys.COMPONENT_INSTANCES, "0",
               ARG_RES_COMP_OPT, EXT_SLEEP_LONG, ResourceKeys.COMPONENT_INSTANCES, "2",
    ])

    ensureApplicationIsUp(NAME)
    status(0, NAME)

    cd = execStatus(NAME)

    assert 5 == cd.statistics.size()
    assert cd.statistics.keySet().containsAll([SliderKeys.COMPONENT_AM,
                                               SLEEP_100, SLEEP_LONG,
                                               EXT_SLEEP_100, EXT_SLEEP_LONG])

    expectLiveContainerCountReached(NAME, SLEEP_LONG, 0,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NAME, EXT_SLEEP_LONG, 2,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NAME, SLEEP_100, 0,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NAME, EXT_SLEEP_100, 0,
      CONTAINER_LAUNCH_TIMEOUT)

    cleanup(NAME)

    describe NESTED_NAME

    slider(0, [ACTION_BUILD, NAME, ARG_METAINFO, TEST_METAINFO,
               ARG_TEMPLATE, TEST_APPCONFIG, ARG_RESOURCES, TEST_RESOURCES])

    slider(0, [ACTION_CREATE, NESTED_NAME, ARG_METAINFO, NEST_METAINFO,
               ARG_TEMPLATE, NEST_APPCONFIG, ARG_RESOURCES, NEST_RESOURCES])

    ensureApplicationIsUp(NESTED_NAME)
    status(0, NESTED_NAME)

    cd = execStatus(NESTED_NAME)

    assert 5 == cd.statistics.size()
    assert cd.statistics.keySet().containsAll([SliderKeys.COMPONENT_AM,
                                               NESTED_PREFIX + SLEEP_100,
                                               NESTED_PREFIX + SLEEP_LONG,
                                               NESTED_PREFIX + EXT_SLEEP_100,
                                               NESTED_PREFIX + EXT_SLEEP_LONG])

    expectLiveContainerCountReached(NESTED_NAME, NESTED_PREFIX + SLEEP_LONG, 1,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NESTED_NAME, NESTED_PREFIX + EXT_SLEEP_LONG, 1,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NESTED_NAME, NESTED_PREFIX + SLEEP_100, 0,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(NESTED_NAME, NESTED_PREFIX + EXT_SLEEP_100, 0,
      CONTAINER_LAUNCH_TIMEOUT)
  }
}
