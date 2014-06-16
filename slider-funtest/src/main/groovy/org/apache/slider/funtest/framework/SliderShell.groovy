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

package org.apache.slider.funtest.framework

import org.apache.bigtop.itest.shell.Shell
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.common.tools.SliderUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory


class SliderShell extends Shell {
  private static final Logger log = LoggerFactory.getLogger(SliderShell.class);


  public static final String BASH = '/bin/bash -s'
  
  /**
   * Configuration directory, shared across all instances. Not marked as volatile,
   * assumed set up during @BeforeClass
   */
  public static File confDir;
  
  public static File script;
  
  public static final List<String> slider_classpath_extra = []

  final String command

  /**
   * Build the command
   * @param commands
   */
  SliderShell(Collection<String> commands) {
    super(BASH)
    assert confDir != null;
    assert script != null;
    command = script.absolutePath + " " + commands.join(" ")
  }

  /**
   * Exec the command
   * @return the script exit code
   */
  int execute() {
    String confDirCmd = env(FuntestProperties.ENV_CONF_DIR, confDir)
    log.info(command)
    List<String> commandLine = [
        confDirCmd,
    ]
    if (!slider_classpath_extra.empty) {
      commandLine << env(FuntestProperties.ENV_SLIDER_CLASSPATH_EXTRA,
          SliderUtils.join(slider_classpath_extra, ":", false))
    }
    commandLine << command
    String script = commandLine.join("\n")
    log.debug(script)
    super.exec(script);
    signCorrectReturnCode()
    return ret;
  }

  String env(String var, Object val) {
    return "export " + var + "=${val.toString()};"
  }

  /**
   * Fix up the return code so that a value of 255 is mapped back to -1
   * @return twos complement return code from an unsigned byte
   */
   int signCorrectReturnCode() {
     ret = signCorrect(ret)
   }

  /**
   * Execute expecting a specific exit code
   * @param expectedExitCode the expected exit code
   */
  void execute(int expectedExitCode) {
    execute()
    assertExitCode(expectedExitCode)
  }
  
  /**
   * Exec any slider command
   * @param conf
   * @param commands
   * @return the shell
   */
  public static SliderShell run(int exitCode, Collection<String> commands) {
    SliderShell shell = new SliderShell(commands)
    shell.execute(exitCode);
    return shell
  }

  /**
   * Sign-correct a process exit code
   * @param exitCode the incoming exit code
   * @return the sign-corrected version
   */
  public static int signCorrect(int exitCode) {
    return (exitCode << 24) >> 24;
  }
  
  @Override
  public String toString() {
    return ret + " =>" + command
  }

  /**
   * Dump the command, return code and outputs to the log.
   * stdout is logged at info; stderr at error.
   */
  public void dumpOutput() {
    log.error(toString())
    log.error("return code = ${signCorrectReturnCode()}")
    if (out.size() != 0) {
      log.info("\n<stdout>\n${out.join('\n')}\n</stdout>");
    }
    if (err.size() != 0) {
      log.error("\n<stderr>\n${err.join('\n')}\n</stderr>");
    }
  }
  
  /**
   * Assert the shell exited with a given error code
   * if not the output is printed and an assertion is raised
   * @param errorCode expected error code
   */
  public void assertExitCode(int errorCode) {
    if (this.ret != errorCode) {
      dumpOutput()
      throw new SliderException(ret,
          "Expected exit code of command ${command} : ${errorCode} - actual=${ret}")
      
    }
  }
  

  }
