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


class ShellBase extends Shell {
  private static final Logger log = LoggerFactory.getLogger(ShellBase.class);
  private static final Logger LOG = log;

  public static final String BASH = '/bin/bash -s'
  public static final String CMD = 'cmd'

  /**
   * Environment variables
   */
  private static final Map<String, String> environment = [:]

  private final String command

  /**
   * Build the command
   * @param commands
   */
  ShellBase(String cmd) {
    super(org.apache.hadoop.util.Shell.WINDOWS ? CMD : BASH)
    command = cmd
  }

  /**
   * Exec the command
   * @return the script exit code
   */
  int execute() {
    log.info(command)
    List<String> commandLine = buildEnvCommands()

    commandLine << command
    if (org.apache.hadoop.util.Shell.WINDOWS) {
      // Ensure the errorlevel returned by last call is set for the invoking shell
      commandLine << "@echo ERRORLEVEL=%ERRORLEVEL%"
      commandLine << "@exit %ERRORLEVEL%"
    }
    String script = commandLine.join("\n")
    log.debug(script)
    exec(script);
    signCorrectReturnCode()
    return ret;
  }

  public String getPathElementSeparator() {
    File.pathSeparator
  }

  public static boolean isWindows() {
    return org.apache.hadoop.util.Shell.WINDOWS
  }

  /**
   * Set an environment variable
   * @param var variable name
   * @param val value
   */
  public static void setEnv(String var, Object val) {
    environment[var] = val.toString()
  }

  /**
   * Get an environment variable
   * @param var variable name
   * @return the value or null
   */
  public static String getEnv(String var) {
    return environment[var]
  }

  /**
   * Build up a list of environment variable setters from the
   * env variables
   * @return a list of commands to set up the env on the target system.
   */
  public static List<String> buildEnvCommands() {
    List<String> commands = []
    environment.each { String var, String val ->
      commands << env(var, val)
    }
    return commands
  }

  /**
   * Add an environment variable
   * @param var variable
   * @param val value (which will be stringified)
   * @return an env variable command
   */
  static String env(String var, Object val) {
    if (isWindows()) {
      return "set " + var + "=${val.toString()}"
    } else {
      return "export " + var + "=${val.toString()};"
    }
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
      log.info("\n<stdout>\n${stdoutHistory}\n</stdout>");
    }
    if (err.size() != 0) {
      log.error("\n<stderr>\n${stdErrHistory}\n</stderr>");
    }
  }

  /**
   * Get the stderr history
   * @return the history
   */
  public String getStdErrHistory() {
    return err.join('\n')
  }

  /**
   * Get the stdout history
   * @return the history
   */
  public String getStdoutHistory() {
    return out.join('\n')
  }

  /**
   * Assert the shell exited with a given error code
   * if not the output is printed and an assertion is raised
   * @param errorCode expected error code
   */
  public void assertExitCode(int errorCode, String extra="") {
    if (this.ret != errorCode) {
      dumpOutput()
      throw new SliderException(ret,
          "Expected exit code of command ${command} : ${errorCode} - actual=${ret} $extra")
    }
  }

  /**
   * Execute shell script consisting of as many Strings as we have arguments,
   * NOTE: individual strings are concatenated into a single script as though
   * they were delimited with new line character. All quoting rules are exactly
   * what one would expect in standalone shell script.
   *
   * After executing the script its return code can be accessed as getRet(),
   * stdout as getOut() and stderr as getErr(). The script itself can be accessed
   * as getScript()
   * WARNING: it isn't thread safe
   * @param args shell script split into multiple Strings
   * @return Shell object for chaining
   */
  Shell exec(Object... args) {
    Process proc = "$shell".execute()
    script = args.join("\n")
    ByteArrayOutputStream baosErr = new ByteArrayOutputStream(4096);
    ByteArrayOutputStream baosOut = new ByteArrayOutputStream(4096);
    proc.consumeProcessOutput(baosOut, baosErr)

    Thread.start {
      def writer = new PrintWriter(new BufferedOutputStream(proc.out))
      writer.println(script)
      writer.flush()
      writer.close()
    }

    proc.waitFor()
    ret = proc.exitValue()

    out = streamToLines(baosOut)
    err = streamToLines(baosErr)

    if (LOG.isTraceEnabled()) {
      if (ret != 0) {
        LOG.trace("return: $ret");
      }
      if (out.size() != 0) {
        LOG.trace("\n<stdout>\n${out.join('\n')}\n</stdout>");
      }

      if (err.size() != 0) {
        LOG.trace("\n<stderr>\n${err.join('\n')}\n</stderr>");
      }
    }
    return this
  }

  /**
   * Convert a stream to lines in an array
   * @param out output stream
   * @return the list of entries
   */
  protected List<String> streamToLines(ByteArrayOutputStream out) {
    if (out.size() != 0) {
      return out.toString().split('\n');
    } else {
      return [];
    }
  }

  public String findLineEntry(String[] locaters) {
    int index = 0;
    def output = out +"\n"+ err
    for (String str in output) {
      if (str.contains("\"" + locaters[index] + "\"")) {
        if (locaters.size() == index + 1) {
          return str;
        } else {
          index++;
        }
      }
    }

    return null;
  }

  public boolean outputContains(
      String lookThisUp,
      int n = 1) {
    int count = 0
    def output = out + "\n" + err
    for (String str in output) {
      int subCount = countString(str, lookThisUp)
      count = count + subCount
      if (count == n) {
        return true;
      }
    }
    return false;
  }

  public static int countString(String str, String search) {
    int count = 0
    if (SliderUtils.isUnset(str) || SliderUtils.isUnset(search)) {
      return count
    }

    int index = str.indexOf(search, 0)
    while (index >= 0) {
      ++count
      index = str.indexOf(search, index + 1)
    }
    return count
  }

  public findLineEntryValue(String[] locaters) {
    String line = findLineEntry(locaters);

    if (line != null) {
      log.info("Parsing {} for value.", line)
      int dividerIndex = line.indexOf(":");
      if (dividerIndex > 0) {
        String value = line.substring(dividerIndex + 1).trim()
        if (value.endsWith(",")) {
          value = value.subSequence(0, value.length() - 1)
        }
        return value;
      }
    }
    return null;
  }

}
