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
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.params.ActionClientArgs;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.providers.agent.application.metadata.Application;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.Package;
import org.apache.slider.test.SliderTestUtils;
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
@PrepareForTest({ProviderUtils.class, ProcessBuilder.class, AgentClientProvider.class, RegistryUtils.class})
public class TestAgentClientProvider2 extends SliderTestUtils {
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
    global.put("e", "{app_name}");
    inputConfig.put("global", global);

    Metainfo metainfo = new Metainfo();
    Application app = new Application();
    metainfo.setApplication(app);
    Package pkg = new Package();
    pkg.setName("app.tar");
    pkg.setType("tarball");
    app.getPackages().add(pkg);

    // This is not a windows path, which is something we bear in mind in testisng
    File clientInstallPath = new File("/tmp/file1");
    String appName = "name";
    String user = "username";

    PowerMock.mockStaticPartial(RegistryUtils.class, "currentUser");
    expect(RegistryUtils.currentUser()).andReturn(user).times(2);
    PowerMock.replay(RegistryUtils.class);

    JSONObject output = provider.getCommandJson(defaultConfig,
                                                inputConfig,
                                                metainfo,
                                                clientInstallPath,
                                                appName);
    JSONObject outConfigs = output.getJSONObject("configurations");
    assertNotNull("null configurations section", outConfigs);
    JSONObject outGlobal = outConfigs.getJSONObject("global");
    assertNotNull("null globals section", outGlobal);
    assertEquals("b", outGlobal.getString("a"));
    assertContained("file1/d", outGlobal.getString("d"));
    assertContained(clientInstallPath.getAbsolutePath(),
        outGlobal.getString("app_install_dir"));
    assertEquals("name", outGlobal.getString("e"));
    assertEquals("name", outGlobal.getString("app_name"));
    assertEquals(user, outGlobal.getString("app_user"));

    defaultConfig = new JSONObject();
    global = new JSONObject();
    global.put("a1", "b2");
    global.put("a", "b-not");
    global.put("d1", "{app_install_dir}/d");
    global.put("e", "{app_name}");
    defaultConfig.put("global", global);

    output = provider.getCommandJson(defaultConfig,
        inputConfig,
        metainfo,
        clientInstallPath,
        null);
    outConfigs = output.getJSONObject("configurations");
    assertNotNull("null configurations section", outConfigs);
    outGlobal = outConfigs.getJSONObject("global");
    assertNotNull("null globals section", outGlobal);
    assertEquals("b", outGlobal.getString("a"));
    assertEquals("b2", outGlobal.getString("a1"));


    assertContained("file1/d", outGlobal.getString("d"));
    assertContained("file1/d", outGlobal.getString("d1"));
    assertContained(clientInstallPath.getAbsolutePath(),
        outGlobal.getString("app_install_dir"));
    assertEquals("{app_name}", outGlobal.getString("e"));
    assertFalse("no 'app_name' field", outGlobal.has("app_name"));
    assertEquals(user, outGlobal.getString("app_user"));

    PowerMock.verify(RegistryUtils.class);
  }

  public void assertContained(String expected, String actual) {
    assertNotNull(actual);
    assertTrue(
        String.format("Did not find \"%s\" in \"%s\"", expected, actual),
        actual.contains(expected));
  }

  @Test
  public void testRunCommand() throws Exception {
    AgentClientProvider provider = new AgentClientProvider(null);
    File appPkgDir = new File("/tmp/pkg");
    File agentPkgDir = new File("/tmp/agt");
    File cmdDir = new File("/tmp/cmd");
    String client_script = "scripts/abc.py";

    List<String> commands =
        Arrays.asList("python", "-S", new File("/tmp/pkg").getAbsolutePath() + File.separator + "package" +
            File.separator + "scripts/abc.py", 
        "INSTALL", new File("/tmp/cmd").getAbsolutePath() + File.separator + "command.json",
                      new File("/tmp/pkg").getAbsolutePath() + File.separator + "package", 
                      new File("/tmp/cmd").getAbsolutePath() + File.separator + "command-out.json", "DEBUG");
    
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

    args.install = true;
    try {
      client.actionClient(args);
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          SliderClient.E_INVALID_INSTALL_LOCATION);
    }

    File tmpFile = File.createTempFile("del", "");
    File dest = new File(tmpFile.getParentFile(), tmpFile.getName() + "dir");
    args.installLocation = dest;
    try {
      client.actionClient(args);
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          SliderClient.E_INSTALL_PATH_DOES_NOT_EXIST);
    }

    dest.mkdir();
    try {
      client.actionClient(args);
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          SliderClient.E_INVALID_APPLICATION_PACKAGE_LOCATION);
    }

    tmpFile = File.createTempFile("del", ".zip");
    args.packageURI = tmpFile.toString();
    args.clientConfig = tmpFile;
    try {
      client.actionClient(args);
    } catch (BadConfigException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_BAD_CONFIGURATION,
          SliderClient.E_MUST_BE_A_VALID_JSON_FILE);
    }

    args.clientConfig = null;
    try {
      client.actionClient(args);
    } catch (BadConfigException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_BAD_CONFIGURATION,
          AgentClientProvider.E_COULD_NOT_READ_METAINFO);
    }
  }
}
