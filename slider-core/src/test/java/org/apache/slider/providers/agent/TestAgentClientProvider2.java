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
package org.apache.slider.providers.agent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.providers.ProviderUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ProviderUtils.class)
public class TestAgentClientProvider2 {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAgentClientProvider2.class);
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testPrepareAMAndConfigForLaunch() throws Exception {
    AgentClientProvider provider = new AgentClientProvider(null);
    SliderFileSystem sfs = PowerMock.createMock(SliderFileSystem.class);
    FileSystem fs = PowerMock.createMock(FileSystem.class);
    Configuration serviceConf = PowerMock.createMock(Configuration.class);
    PowerMock.mockStatic(ProviderUtils.class);

    expect(sfs.getFileSystem()).andReturn(fs);
    expect(fs.mkdirs(anyObject(Path.class))).andReturn(true);
    expect(ProviderUtils.addAgentTar(
        anyObject(), anyObject(String.class), anyObject(SliderFileSystem.class), anyObject(Path.class))).
        andReturn(true);

    AggregateConf instanceDefinition = new AggregateConf();
    ConfTree tree = new ConfTree();
    tree.global.put(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH, ".");
    instanceDefinition.setInternal(tree);

    PowerMock.replay(sfs, fs, serviceConf, ProviderUtils.class);

    provider.prepareAMAndConfigForLaunch(
        sfs, serviceConf, null, instanceDefinition, null,
        null, null, null, null, false);

    Assert.assertTrue(tree.global.containsKey(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH));
    tree.global.remove(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);

    // Verify that slider-agent.tar.gz got added
    Path tempPath = new Path(".", "temp");
    provider.prepareAMAndConfigForLaunch(
        sfs, serviceConf, null, instanceDefinition, null,
        null, null, null, tempPath, false);
    PowerMock.verify(sfs, fs, ProviderUtils.class);
    Assert.assertTrue(tree.global.containsKey(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH));
  }
}
