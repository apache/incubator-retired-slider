/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.chaos.remote

import com.jcraft.jsch.JSch
import com.jcraft.jsch.Session
import com.jcraft.jsch.UIKeyboardInteractive
import com.jcraft.jsch.UserInfo
import groovy.util.logging.Commons
import org.apache.commons.logging.LogFactory

/**
 * A remote server you can SSH to
 */
@Commons
class RemoteServer implements UserInfo, UIKeyboardInteractive {


  boolean publicKeyAuth = true
  File publicKeyFile
  File knownHosts
  String username
  String password
  String host
  int port = 22
  int timeout = 60000
  int connectTimeout = 10000
  private JSch jsch

  @Override
  String getPassphrase() {
    return password
  }

  @Override
  boolean promptPassword(String message) {
    log.debug("Password prompt: $message")
    return false
  }

  @Override
  boolean promptPassphrase(String message) {
    log.debug("Passphrase prompt: $message")
    return false
  }

  /**
   * This is for the "do you trust this host" message. We do
   * @param message
   * @return
   */
  @Override
  boolean promptYesNo(String message) {
    log.debug("YesNo prompt: $message")
    return true
  }

  @Override
  String[] promptKeyboardInteractive(String destination, String name, String instruction, String[] prompt, boolean[] echo) {
    return new String[0]
  }

  @Override
  void showMessage(String message) {
    log.info(message)
  }

  private synchronized JSch createJschInstance() {
    if (jsch) {
      return jsch
    }
    jsch = new JSch()
    //do not try and convert a groovy shortcut as overloaded methods then cause confusion
    jsch.setLogger(new JschToCommonsLog(LogFactory.getLog("org.apache.chaos.remote.RemoteServer.ssh")))
    if (publicKeyAuth) {
      assert publicKeyFile
      assert publicKeyFile.exists() && publicKeyFile.canRead()
      jsch.addIdentity(publicKeyFile.toString(), (String) password);
    } else {
      assert password != null
    }
    if (knownHosts) {
      assert knownHosts.exists() && knownHosts.canRead()
      jsch.setKnownHosts(knownHosts.toString())
    }
    jsch
  }

  public JSch getJschInstance() {
    return jsch ?: createJschInstance()
  }

  Session connect(JSch jsch, String host, int port) {
    assert host
    Session session = jsch.getSession(username, host, port)
    session.userInfo = this
    if (!publicKeyAuth) {
      session.setPassword(password)
    }
    session.timeout = timeout
    session.connect(connectTimeout)
    session
  }

  /**
   * Connect to the (host, port)
   * @return a new session
   */
  Session connect() {
    return connect(jschInstance, host, port)
  }

  /**
   * Connect to the server and issue the commands, one per line. Nest arguments
   * in inner lists for better structure
   * @param args multi-dimensional list
   * @return ( return value , output )
   */
  List exec(List args) {
    def session = connect()
    SshCommands cmds = new SshCommands(session)
    return cmds.exec(args)
  }

  List command(String command) {
    def session = connect()
    SshCommands cmds = new SshCommands(session)
    def (status, text) = cmds.command([command])
    log.info("$status : $text")
    [status, text]
  }

  /**
   * Connect to a host and kill a process there
   * @param pidFile the PID file
   * @param signal the signal
   * @return the outcome of the command
   */
  List kill(int signal, String pidFile) {
    return command("kill -$signal `cat $pidFile`");
  }

  Clustat clustat() {
    def (status, text) = command("clustat -x")
    if (status != 0) {
      throw new IOException("Clustat command failed [$status]: $text")
    }
    Clustat clustat = new Clustat(text)
    return clustat
  }

  void waitForServerLive(int timeout) {
    long endtime = System.currentTimeMillis() + timeout
    boolean live = false;

    while (!live && System.currentTimeMillis() < endtime) {
      try {
        command("true")
        live = true
        break
      } catch (IOException ignored) {
        Thread.sleep(1000)
      }

    }

  }

  @Override
  String toString() {
    return "Remote server user=$username @ host=$host"
  }
}
