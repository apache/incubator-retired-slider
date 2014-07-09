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

import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.conf.MapOperations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 *
 */
public class TestCertificateManager {
  @Rule
  public TemporaryFolder workDir = new TemporaryFolder();
  private File secDir;

  @Before
  public void setup() throws Exception {
    CertificateManager certMan = new CertificateManager();
    MapOperations compOperations = new MapOperations();
    secDir = new File(workDir.getRoot(), SliderKeys.SECURITY_DIR);
    File keystoreFile = new File(secDir, SliderKeys.KEYSTORE_FILE_NAME);
    compOperations.put(SliderKeys.KEYSTORE_LOCATION,
                       keystoreFile.getAbsolutePath());
    certMan.initRootCert(compOperations);
  }

  @Test
  public void testServerCertificateGenerated() throws Exception {
    File serverCrt = new File(secDir, SliderKeys.CRT_FILE_NAME);
    Assert.assertTrue(serverCrt.exists());
  }

  @Test
  public void testKeystoreGenerated() throws Exception {
    File keystore = new File(secDir, SliderKeys.KEYSTORE_FILE_NAME);
    Assert.assertTrue(keystore.exists());
  }

}
