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
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.slider.api.ResourceKeys
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation
import org.junit.Test

/**
 * Test the container resource allocation logic
 */
@CompileStatic
@Slf4j
class TestMockContainerResourceAllocations extends BaseMockAppStateTest {

  @Override
  String getTestName() {
    "TestMockContainerResourceAllocations"
  }

  @Test
  public void testNormalAllocations() throws Throwable {
    ConfTree clusterSpec = factory.newConfTree(1, 0, 0)
    ConfTreeOperations cto = new ConfTreeOperations(clusterSpec)

    cto.setComponentOpt(MockRoles.ROLE0, ResourceKeys.YARN_MEMORY, 512)
    cto.setComponentOpt(MockRoles.ROLE0, ResourceKeys.YARN_CORES, 2)
    appState.updateResourceDefinitions(clusterSpec)
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    Resource requirements = operation.request.capability
    assert requirements.memory == 512
    assert requirements.virtualCores == 2
  }

  @Test
  public void testMaxMemAllocations() throws Throwable {
    ConfTree clusterSpec = factory.newConfTree(1, 0, 0)
    ConfTreeOperations cto = new ConfTreeOperations(clusterSpec)

    cto.setComponentOpt(MockRoles.ROLE0, ResourceKeys.YARN_MEMORY,
                           ResourceKeys.YARN_RESOURCE_MAX)
    cto.setComponentOpt(MockRoles.ROLE0, ResourceKeys.YARN_CORES, 2)
    appState.updateResourceDefinitions(clusterSpec)
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    Resource requirements = operation.request.capability
    assert requirements.memory == RM_MAX_RAM
    assert requirements.virtualCores == 2
  }
  
  @Test
  public void testMaxCoreAllocations() throws Throwable {
    ConfTree clusterSpec = factory.newConfTree(1, 0, 0)
    ConfTreeOperations cto = new ConfTreeOperations(clusterSpec)
    cto.setComponentOpt(MockRoles.ROLE0, ResourceKeys.YARN_MEMORY,
        512)
    cto.setComponentOpt(MockRoles.ROLE0, ResourceKeys.YARN_CORES,
        ResourceKeys.YARN_RESOURCE_MAX)
    appState.updateResourceDefinitions(clusterSpec)
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    Resource requirements = operation.request.capability
    assert requirements.memory == 512
    assert requirements.virtualCores == RM_MAX_CORES
  }
  
  @Test
  public void testMaxDefaultAllocations() throws Throwable {

    ConfTree clusterSpec = factory.newConfTree(1, 0, 0)
    appState.updateResourceDefinitions(clusterSpec)
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    Resource requirements = operation.request.capability
    assert requirements.memory == ResourceKeys.DEF_YARN_MEMORY
    assert requirements.virtualCores == ResourceKeys.DEF_YARN_CORES
  }

}
