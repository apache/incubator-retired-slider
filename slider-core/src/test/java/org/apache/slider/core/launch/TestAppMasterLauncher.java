/**
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

package org.apache.slider.core.launch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.client.SliderYarnClientImpl;
import org.apache.slider.common.SliderKeys;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAppMasterLauncher {
  SliderYarnClientImpl mockYarnClient;
  YarnClientApplication yarnClientApp;
  ApplicationSubmissionContext appSubmissionContext;
  Set<String> tags = Collections.emptySet();

  @Before
  public void initialize() throws Exception {
    mockYarnClient = EasyMock.createNiceMock(SliderYarnClientImpl.class);
    yarnClientApp = EasyMock.createNiceMock(YarnClientApplication.class);
    appSubmissionContext = EasyMock
        .createNiceMock(ApplicationSubmissionContext.class);
    EasyMock.expect(yarnClientApp.getApplicationSubmissionContext())
        .andReturn(appSubmissionContext).once();
    EasyMock.expect(mockYarnClient.createApplication())
        .andReturn(yarnClientApp).once();
  }

  @Test
  public void testExtractLogAggregationContext() throws Exception {
    Map<String, String> options = new HashMap<String, String>();
    options.put(ResourceKeys.YARN_LOG_INCLUDE_PATTERNS,
        " | slider*.txt  |agent.out| |");
    options.put(ResourceKeys.YARN_LOG_EXCLUDE_PATTERNS,
        "command*.json|  agent.log*        |     ");
    options.put(ResourceKeys.YARN_LOG_INTERVAL, "30");

    EasyMock.replay(mockYarnClient, appSubmissionContext, yarnClientApp);
    AppMasterLauncher appMasterLauncher = new AppMasterLauncher("cl1",
        SliderKeys.APP_TYPE, null, null, mockYarnClient, false, null, options,
        tags);

    // Verify the include/exclude patterns
    String expectedInclude = "slider*.txt|agent.out";
    Assert.assertEquals(expectedInclude,
        appMasterLauncher.logAggregationContext.getIncludePattern());

    String expectedExclude = "command*.json|agent.log*";
    Assert.assertEquals(expectedExclude,
        appMasterLauncher.logAggregationContext.getExcludePattern());

    Assert.assertEquals(30,
        appMasterLauncher.logAggregationContext.getRollingIntervalSeconds());

    EasyMock.verify(mockYarnClient, appSubmissionContext, yarnClientApp);
  }

  @Test
  public void testExtractLogAggregationContextEmptyIncludePattern()
    throws Exception {
    Map<String, String> options = new HashMap<String, String>();
    options.put(ResourceKeys.YARN_LOG_INCLUDE_PATTERNS, " ");
    options.put(ResourceKeys.YARN_LOG_EXCLUDE_PATTERNS,
        "command*.json|  agent.log*        |     ");
    options.put(ResourceKeys.YARN_LOG_INTERVAL, "600");

    EasyMock.replay(mockYarnClient, appSubmissionContext, yarnClientApp);
    AppMasterLauncher appMasterLauncher = new AppMasterLauncher("cl1",
        SliderKeys.APP_TYPE, null, null, mockYarnClient, false, null, options,
        tags);

    // Verify the include/exclude patterns
    String expectedInclude = "";
    Assert.assertEquals(expectedInclude,
        appMasterLauncher.logAggregationContext.getIncludePattern());

    String expectedExclude = "command*.json|agent.log*";
    Assert.assertEquals(expectedExclude,
        appMasterLauncher.logAggregationContext.getExcludePattern());

    Assert.assertEquals(600,
        appMasterLauncher.logAggregationContext.getRollingIntervalSeconds());

    EasyMock.verify(mockYarnClient, appSubmissionContext, yarnClientApp);
  }

}
