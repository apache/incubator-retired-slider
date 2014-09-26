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
package org.apache.slider.server.services.security;

import com.google.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.MessageFormat;

@Singleton
public class CertificateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(CertificateManager.class);

  private static final String GEN_SRVR_KEY = "openssl genrsa -des3 " +
      "-passout pass:{0} -out {1}/{2} 4096 ";
  private static final String GEN_SRVR_REQ = "openssl req -passin pass:{0} " +
      "-new -key {1}/{2} -out {1}/{5} -batch";
  private static final String SIGN_SRVR_CRT = "openssl ca -create_serial " +
    "-out {1}/{3} -days 365 -keyfile {1}/{2} -key {0} -selfsign " +
    "-extensions jdk7_ca -config {1}/ca.config -batch " +
    "-infiles {1}/{5}";
  private static final String EXPRT_KSTR = "openssl pkcs12 -export" +
      " -in {1}/{3} -inkey {1}/{2} -certfile {1}/{3} -out {1}/{4} " +
      "-password pass:{0} -passin pass:{0} \n";
  private static final String REVOKE_AGENT_CRT = "openssl ca " +
      "-config {0}/ca.config -keyfile {0}/{4} -revoke {0}/{2} -batch " +
      "-passin pass:{3} -cert {0}/{5}";
  private static final String SIGN_AGENT_CRT = "openssl ca -config " +
      "{0}/ca.config -in {0}/{1} -out {0}/{2} -batch -passin pass:{3} " +
      "-keyfile {0}/{4} -cert {0}/{5}"; /**
       * Verify that root certificate exists, generate it otherwise.
       */
  public void initRootCert(MapOperations compOperations) {
    SecurityUtils.initializeSecurityParameters(compOperations);

    LOG.info("Initialization of root certificate");
    boolean certExists = isCertExists();
    LOG.info("Certificate exists:" + certExists);

    if (!certExists) {
      generateServerCertificate();
    }

  }

  /**
   * Checks root certificate state.
   * @return "true" if certificate exists
   */
  private boolean isCertExists() {

    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String srvrCrtName = SliderKeys.CRT_FILE_NAME;
    File certFile = new File(srvrKstrDir + File.separator + srvrCrtName);
    LOG.debug("srvrKstrDir = " + srvrKstrDir);
    LOG.debug("srvrCrtName = " + srvrCrtName);
    LOG.debug("certFile = " + certFile.getAbsolutePath());

    return certFile.exists();
  }

  class StreamConsumer extends Thread
  {
    InputStream is;
    boolean logOutput;

    StreamConsumer(InputStream is, boolean logOutput)
    {
      this.is = is;
      this.logOutput = logOutput;
    }

    StreamConsumer(InputStream is)
    {
      this(is, false);
    }

    public void run()
    {
      try
      {
        InputStreamReader isr = new InputStreamReader(is,
                                                      Charset.forName("UTF8"));
        BufferedReader br = new BufferedReader(isr);
        String line;
        while ( (line = br.readLine()) != null)
          if (logOutput) {
            LOG.info(line);
          }
      } catch (IOException e)
      {
        LOG.error("Error during processing of process stream", e);
      }
    }
  }


  /**
   * Runs os command
   *
   * @return command execution exit code
   */
  private int runCommand(String command) throws SliderException {
    int exitCode = -1;
    String line = null;
    Process process = null;
    BufferedReader br= null;
    try {
      process = Runtime.getRuntime().exec(command);
      StreamConsumer outputConsumer =
          new StreamConsumer(process.getInputStream(), true);
      StreamConsumer errorConsumer =
          new StreamConsumer(process.getErrorStream());

      outputConsumer.start();
      errorConsumer.start();

      try {
        process.waitFor();
        SecurityUtils.logOpenSslExitCode(command, process.exitValue());
        exitCode = process.exitValue();
        if (exitCode != 0) {
          throw new SliderException(exitCode, "Error running command {}", command);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
    }

    return exitCode;//some exception occurred

  }

  private void generateServerCertificate(){
    LOG.info("Generation of server certificate");

    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String srvrCrtName = SliderKeys.CRT_FILE_NAME;
    String srvrCsrName = SliderKeys.CSR_FILE_NAME;
    String srvrKeyName = SliderKeys.KEY_FILE_NAME;
    String kstrName = SliderKeys.KEYSTORE_FILE_NAME;
    String srvrCrtPass = SecurityUtils.getKeystorePass();

    Object[] scriptArgs = {srvrCrtPass, srvrKstrDir, srvrKeyName,
        srvrCrtName, kstrName, srvrCsrName};

    try {
      String command = MessageFormat.format(GEN_SRVR_KEY,scriptArgs);
      runCommand(command);

      command = MessageFormat.format(GEN_SRVR_REQ,scriptArgs);
      runCommand(command);

      command = MessageFormat.format(SIGN_SRVR_CRT,scriptArgs);
      runCommand(command);

      command = MessageFormat.format(EXPRT_KSTR,scriptArgs);
      runCommand(command);
    } catch (SliderException e) {
      LOG.error("Error generating the server certificate", e);
    }

  }

  /**
   * Returns server certificate content
   * @return string with server certificate content
   */
  public String getServerCert() {
    File certFile = new File(SecurityUtils.getSecurityDir() +
        File.separator + SliderKeys.CRT_FILE_NAME);
    String srvrCrtContent = null;
    try {
      srvrCrtContent = FileUtils.readFileToString(certFile);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return srvrCrtContent;
  }

  /**
   * Signs agent certificate
   * Adds agent certificate to server keystore
   * @return string with agent signed certificate content
   */
  public synchronized SignCertResponse signAgentCrt(String agentHostname,
                                                    String agentCrtReqContent,
                                                    String passphraseAgent) {
    SignCertResponse response = new SignCertResponse();
    LOG.info("Signing of agent certificate");
    LOG.info("Verifying passphrase");

    String passphraseSrvr = SliderKeys.PASSPHRASE;

    if (!passphraseSrvr.equals(passphraseAgent.trim())) {
      LOG.warn("Incorrect passphrase from the agent");
      response.setResult(SignCertResponse.ERROR_STATUS);
      response.setMessage("Incorrect passphrase from the agent");
      return response;
    }

    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String srvrCrtPass = SecurityUtils.getKeystorePass();
    String srvrCrtName = SliderKeys.CRT_FILE_NAME;
    String srvrKeyName = SliderKeys.KEY_FILE_NAME;
    String agentCrtReqName = agentHostname + ".csr";
    String agentCrtName = agentHostname + ".crt";

    Object[] scriptArgs = {srvrKstrDir, agentCrtReqName, agentCrtName,
        srvrCrtPass, srvrKeyName, srvrCrtName};

    //Revoke previous agent certificate if exists
    File agentCrtFile = new File(srvrKstrDir + File.separator + agentCrtName);

    String command = null;
    if (agentCrtFile.exists()) {
      LOG.info("Revoking of " + agentHostname + " certificate.");
      command = MessageFormat.format(REVOKE_AGENT_CRT, scriptArgs);
      try {
        runCommand(command);
      } catch (SliderException e) {
        int commandExitCode = e.getExitCode();
        response.setResult(SignCertResponse.ERROR_STATUS);
        response.setMessage(
            SecurityUtils.getOpenSslCommandResult(command, commandExitCode));
        return response;
      }
    }

    File agentCrtReqFile = new File(srvrKstrDir + File.separator +
        agentCrtReqName);
    try {
      FileUtils.writeStringToFile(agentCrtReqFile, agentCrtReqContent);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    command = MessageFormat.format(SIGN_AGENT_CRT, scriptArgs);

    LOG.debug(SecurityUtils.hideOpenSslPassword(command));
    try {
      runCommand(command);
    } catch (SliderException e) {
      int commandExitCode = e.getExitCode();
      response.setResult(SignCertResponse.ERROR_STATUS);
      response.setMessage(
          SecurityUtils.getOpenSslCommandResult(command, commandExitCode));
      return response;
    }

    String agentCrtContent = "";
    try {
      agentCrtContent = FileUtils.readFileToString(agentCrtFile);
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error("Error reading signed agent certificate");
      response.setResult(SignCertResponse.ERROR_STATUS);
      response.setMessage("Error reading signed agent certificate");
      return response;
    }
    response.setResult(SignCertResponse.OK_STATUS);
    response.setSignedCa(agentCrtContent);
    //LOG.info(ShellCommandUtil.getOpenSslCommandResult(command, commandExitCode));
    return response;
  }
}
