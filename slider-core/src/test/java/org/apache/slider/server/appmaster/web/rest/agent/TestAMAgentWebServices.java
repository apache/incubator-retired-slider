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

package org.apache.slider.server.appmaster.web.rest.agent;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.PortScanner;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.model.mock.MockAppState;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockProviderService;
import org.apache.slider.server.appmaster.model.mock.MockClusterServices;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.state.SimpleReleaseSelector;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.WebAppApiImpl;
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.services.security.CertificateManager;
import org.apache.slider.server.services.security.SecurityUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestAMAgentWebServices {

  static CertificateManager certificateManager;
  
  static {
    //for localhost testing only
    javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
        new javax.net.ssl.HostnameVerifier(){

          public boolean verify(String hostname,
                                javax.net.ssl.SSLSession sslSession) {
            return hostname.equals("localhost");
          }
        });

  }

  protected static final Logger log =
    LoggerFactory.getLogger(TestAMAgentWebServices.class);
  
  public static final String AGENT_URL =
    "https://localhost:${PORT}/ws/v1/slider/agents/";
  public static final int MIN_PORT = 50000;
  public static final int MAX_PORT = 50050;
  
  static MockFactory factory = new MockFactory();
  private static WebAppApi slider;

  private static FileSystem fs;
  private AgentWebApp webApp;
  private String base_url;

  @BeforeClass
  public static void setupClass() throws SliderException {
    MapOperations configMap = new MapOperations();
    SecurityUtils.initializeSecurityParameters(configMap, true);
    certificateManager = new CertificateManager();
    certificateManager.initialize(configMap);
    String keystoreFile = SecurityUtils.getSecurityDir() + File.separator +
                          SliderKeys.KEYSTORE_FILE_NAME;
    String password = SecurityUtils.getKeystorePass();
    System.setProperty("javax.net.ssl.trustStore", keystoreFile);
    System.setProperty("javax.net.ssl.trustStorePassword", password);
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
  }
  

  @Before
  public void setUp() throws Exception {
    YarnConfiguration conf = SliderUtils.createConfiguration();
    fs = FileSystem.get(new URI("file:///"), conf);
    AppState appState = null;
    File historyWorkDir = new File("target/history", "TestAMAgentWebServices");
    Path historyPath = new Path(historyWorkDir.toURI());
    fs.delete(historyPath, true);
    appState = new MockAppState(new MockClusterServices());
    AppStateBindingInfo binding = new AppStateBindingInfo();
    binding.instanceDefinition = factory.newInstanceDefinition(0, 0, 0);
    binding.roles = MockFactory.ROLES;
    binding.fs = fs;
    binding.historyPath = historyPath;
    appState.buildInstance(binding);
    ProviderAppState providerAppState = new ProviderAppState("undefined",
                                                             appState);

    slider = new WebAppApiImpl(providerAppState,
                               new MockProviderService(), null, null, null,
        null, null, null);

    MapOperations compOperations = new MapOperations();

    PortScanner portScanner = new PortScanner();
    portScanner.setPortRange(Integer.toString(MIN_PORT) + "-" + Integer.toString(MAX_PORT));

    webApp = AgentWebApp.$for(AgentWebApp.BASE_PATH, slider,
                              RestPaths.WS_AGENT_CONTEXT_ROOT)
        .withComponentConfig(compOperations)
        .withPort(portScanner.getAvailablePort())
        .withSecuredPort(portScanner.getAvailablePort())
        .start();
    base_url = AGENT_URL.replace("${PORT}",
                                 Integer.toString(webApp.getSecuredPort()));

  }

  @After
  public void tearDown () throws Exception {
    IOUtils.closeStream(webApp);
    webApp = null;
  }

  public TestAMAgentWebServices() {
  }

  @Test
  public void testRegistration() throws Exception {
    RegistrationResponse response;
    Client client = createTestClient();
    WebResource webResource = client.resource(base_url + "test/register");
    response = webResource.type(MediaType.APPLICATION_JSON)
        .post(RegistrationResponse.class, createDummyJSONRegister());
    assertEquals(RegistrationStatus.OK, response.getResponseStatus());
  }

  protected Client createTestClient() {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    return Client.create(clientConfig);
  }

  @Test
  public void testHeartbeat() throws Exception {
    HeartBeatResponse response;
    Client client = createTestClient();
    WebResource webResource = client.resource(base_url + "test/heartbeat");
    response = webResource.type(MediaType.APPLICATION_JSON)
        .post(HeartBeatResponse.class, createDummyHeartBeat());
    assertEquals(response.getResponseId(), 0L);
  }

  @Test
  public void testHeadURL() throws Exception {
    Client client = createTestClient();
    WebResource webResource = client.resource(base_url);
    ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
                                         .head();
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testAllowedPortRange() throws Exception {
    assertTrue(webApp.getPort() >= MIN_PORT && webApp.getPort() <= MAX_PORT);
  }

  @Test
  public void testAllowedSecurePortRange() throws Exception {
    assertTrue(webApp.getSecuredPort() >= MIN_PORT && webApp.getSecuredPort() <= MAX_PORT);
  }

//  @Test
//  public void testSleepForAWhile() throws Throwable {
//    log.info("Agent is running at {}", base_url);
//    Thread.sleep(60 * 1000);
//  }

  public static void testCleanupSecurityDir() throws Exception {
    // Since initialization is done without setting ssl.server.keystore.location
    // the security dir is created in temp file system
    String securityDir = SecurityUtils.getSecurityDir();
    // validate that the folder exists
    File securityDirFile = new File(securityDir);
    assertTrue("securityDir " + securityDir + " should exist",
        securityDirFile.exists());
    // call cleanup now and it should be gone
    SecurityUtils.cleanupSecurityDir();
    // validate that the folder does not exist anymore
    assertFalse("securityDir " + securityDir + " should have been deleted",
        securityDirFile.exists());
  }

  private Register createDummyJSONRegister() {
    Register register = new Register();
    register.setResponseId(-1);
    register.setTimestamp(System.currentTimeMillis());
    register.setLabel("dummyHost");
    return register;
  }

  private HeartBeat createDummyHeartBeat() {
    HeartBeat json = new HeartBeat();
    json.setResponseId(-1);
    json.setTimestamp(System.currentTimeMillis());
    json.setHostname("dummyHost");
    return json;
  }

  @AfterClass
  public static void tearDownClass() throws Exception{
    testCleanupSecurityDir();
//    Path directory = Paths.get(SecurityUtils.getSecurityDir());
//    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
//      @Override
//      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
//          throws IOException {
//        Files.delete(file);
//        return FileVisitResult.CONTINUE;
//      }
//
//      @Override
//      public FileVisitResult postVisitDirectory(Path dir, IOException exc)
//          throws IOException {
//        Files.delete(dir);
//        return FileVisitResult.CONTINUE;
//      }
//
//    });
  }
}
