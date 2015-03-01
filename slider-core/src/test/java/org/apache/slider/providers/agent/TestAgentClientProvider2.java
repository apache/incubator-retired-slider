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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.params.ActionClientArgs;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.providers.agent.application.metadata.Application;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.Package;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ProviderUtils.class, ProcessBuilder.class, AgentClientProvider.class})
public class TestAgentClientProvider2 {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAgentClientProvider2.class);
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void initialize() {
    BasicConfigurator.resetConfiguration();
    BasicConfigurator.configure();
  }

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

  @Test
  public void testGetCommandJson() throws Exception {
    AgentClientProvider provider = new AgentClientProvider(null);
    JSONObject defaultConfig = null;

    JSONObject inputConfig = new JSONObject();
    JSONObject global = new JSONObject();
    global.put("a", "b");
    global.put("d", "{app_install_dir}/d");
    inputConfig.put("global", global);

    Metainfo metainfo = new Metainfo();
    Application app = new Application();
    metainfo.setApplication(app);
    Package pkg = new Package();
    pkg.setName("app.tar");
    pkg.setType("tarball");
    app.getPackages().add(pkg);

    File clientInstallPath = new File("/tmp/file1");

    JSONObject output = provider.getCommandJson(defaultConfig,
                                                inputConfig,
                                                metainfo,
                                                clientInstallPath);
    JSONObject outConfigs = output.getJSONObject("configurations");
    Assert.assertNotNull(outConfigs);
    JSONObject outGlobal = outConfigs.getJSONObject("global");
    Assert.assertNotNull(outGlobal);
    Assert.assertEquals("b", outGlobal.getString("a"));
    Assert.assertEquals("/tmp/file1/d", outGlobal.getString("d"));

    defaultConfig = new JSONObject();
    global = new JSONObject();
    global.put("a1", "b2");
    global.put("a", "b-not");
    global.put("d1", "{app_install_dir}/d");
    defaultConfig.put("global", global);

    output = provider.getCommandJson(defaultConfig,
                                     inputConfig,
                                     metainfo,
                                     clientInstallPath);
    outConfigs = output.getJSONObject("configurations");
    Assert.assertNotNull(outConfigs);
    outGlobal = outConfigs.getJSONObject("global");
    Assert.assertNotNull(outGlobal);
    Assert.assertEquals("b", outGlobal.getString("a"));
    Assert.assertEquals("/tmp/file1/d", outGlobal.getString("d"));
    Assert.assertEquals("b2", outGlobal.getString("a1"));
    Assert.assertEquals("/tmp/file1/d", outGlobal.getString("d1"));
  }


  @Test
  public void testRunCommand() throws Exception {
    AgentClientProvider provider = new AgentClientProvider(null);
    File appPkgDir = new File("/tmp/pkg");
    File agentPkgDir = new File("/tmp/agt");
    File cmdDir = new File("/tmp/cmd");
    String client_script = "scripts/abc.py";

    List<String> commands =
        Arrays.asList("python", "-S", "/tmp/pkg/package/scripts/abc.py", "INSTALL", "/tmp/cmd/command.json",
                      "/tmp/pkg/package", "/tmp/cmd/command-out.json", "DEBUG");
    ProcessBuilder pbMock = PowerMock.createMock(ProcessBuilder.class);
    Process procMock = PowerMock.createMock(Process.class);
    PowerMock.expectNew(ProcessBuilder.class, commands).andReturn(pbMock);

    expect(pbMock.environment()).andReturn(new HashMap<String, String>()).anyTimes();
    expect(pbMock.start()).andReturn(procMock);
    expect(pbMock.command()).andReturn(new ArrayList<String>());
    expect(procMock.waitFor()).andReturn(0);
    expect(procMock.exitValue()).andReturn(0);
    expect(procMock.getErrorStream()).andReturn(IOUtils.toInputStream("stderr", "UTF-8"));
    expect(procMock.getInputStream()).andReturn(IOUtils.toInputStream("stdout", "UTF-8"));

    PowerMock.replayAll();

    provider.runCommand(appPkgDir,
                        agentPkgDir,
                        cmdDir,
                        client_script);
    PowerMock.verifyAll();
  }

  @Test
  public void testSliderClientForInstallFailures() throws Exception {
    SliderClient client = new SliderClient();
    client.bindArgs(new Configuration(), "client", "--dest", "a_random_path/none", "--package", "a_random_pkg.zip");
    ActionClientArgs args = new ActionClientArgs();
    args.install = false;
    try {
      client.actionClient(args);
    }catch(BadCommandArgumentsException e) {
      log.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("Only install command is supported for the client"));
    }

    args.install = true;
    try {
      client.actionClient(args);
    }catch(BadCommandArgumentsException e) {
      log.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("A valid install location must be provided for the client"));
    }

    File tmpFile = File.createTempFile("del", "");
    File dest = new File(tmpFile.getParentFile(), tmpFile.getName() + "dir");
    args.installLocation = dest;
    try {
      client.actionClient(args);
    }catch(BadCommandArgumentsException e) {
      log.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("Install path does not exist at"));
    }

    dest.mkdir();
    try {
      client.actionClient(args);
    }catch(BadCommandArgumentsException e) {
      log.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("A valid application package location required"));
    }

    tmpFile = File.createTempFile("del", ".zip");
    args.packageURI = tmpFile.toString();
    args.clientConfig = tmpFile;
    try {
      client.actionClient(args);
    }catch(SliderException e) {
      log.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("Invalid configuration. Must be a valid json file"));
    }

    args.clientConfig = null;
    try {
      client.actionClient(args);
    }catch(SliderException e) {
      log.info(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("Not a valid app package. Could not read metainfo.xml"));
    }
  }
}
