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

import com.jcraft.jsch.ChannelExec
import com.jcraft.jsch.ChannelShell
import com.jcraft.jsch.Session
import groovy.util.logging.Commons
import org.apache.tools.ant.util.KeepAliveOutputStream
import org.apache.tools.ant.util.TeeOutputStream

/**
 * Send lists strings of commands 
 */
@Commons
class SshCommands {

  private final Session session
  private volatile boolean stop = false
  private volatile Thread thread
  long commandsTimeout = 10000

  SshCommands(Session session) {
    this.session = session
  }

  private synchronized void begin() {
    thread = Thread.currentThread()
    Thread.start("ssh command waker") {
      sleep(commandsTimeout);
      stop()
    }
  }

  private synchronized end() {
    thread = null
    stop = false
  }

  public synchronized void stop() {
    if (!stop) {
      stop = true;
      if (thread) {
        thread.interrupt()
      }
    }
  }

  /**
   * Execute a list of commands, return the exit status. Output goes to the 
   * output stream
   * @param args , one per line -all converted to strings first. If the list
   * element is itself a collection, it is flattened then joined with spaces. This
   * can be used to build up more complex commands.
   * @return the result of the execution -the last response from the command
   */
  List exec(List args) {
    assert session && session.connected
    List<String> commandSet = args.collect() {
      (it instanceof Collection) ? (it.flatten().join(" ")) : it.toString()
    }
    String commandLine = commandSet.join("\n")
    log.info("commands: $commandLine")
    ChannelShell channel = null
    try {
      begin()
      channel = (ChannelShell) session.openChannel("shell")

      //connect triggers a linkup and the streaming
      channel.connect();
      assert channel.connected
      channel.pty = true
      OutputStream toServer = channel.outputStream
      Reader fromServer = new BufferedReader(
          new InputStreamReader(channel.inputStream, "UTF-8"));
      toServer.write(commandLine.bytes)
      toServer.flush()
      StringBuilder builder = new StringBuilder()

      String line
      while (!channel.closed && !stop) {
        line = fromServer.readLine()
        StringBuilder append = builder.append(line)
        log.debug(line);
      }

      int status = channel.exitStatus
      if (Thread.interrupted()) {
        log.debug("Interrupted while waiting for the end of the SSH stream")
        throw new InterruptedIOException()
      }
      return [status, builder.toString()]
    } finally {
      end()
      boolean interrupted = Thread.interrupted()
      channel?.disconnect()
    }
  }
  /**
   * Execute a single of commands, return the exit status. Output goes to the 
   * output stream
   * @param args -a list that is flattened then joined with spaces. This
   * can be used to build up more complex commands.
   * @return the result of the execution -the response from the command
   */
  List command(String arg) {
    return command([arg])
  }

  List command(List args) {
    assert session && session.connected
    session.timeout = commandsTimeout
    String commandLine = args.join(" ")
    log.info("commands: $commandLine")
    ChannelExec channel = null
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TeeOutputStream tee =
      new TeeOutputStream(out,
                          KeepAliveOutputStream.wrapSystemOut());

    try {
      begin()
      channel = (ChannelExec) session.openChannel("exec")


      channel.setCommand(commandLine);
      channel.setOutputStream(tee);
      channel.setExtOutputStream(tee);
      channel.pty = true

      //connect triggers a linkup and the streaming
      channel.connect();
      assert channel.connected

      Thread thread = Thread.start() {
        while (!channel.isClosed()) {
          if (thread == null) {
            return;
          }
          try {
            sleep(500);
          } catch (Exception e) {
            log.debug("In channel thread $e", e)
            // ignored
          }
        }
      }

      thread.join(commandsTimeout);
      if (thread.isAlive()) {
        // ran out of time
        thread = null;
        throw new IOException("Timeout executing command  " + commandLine);
      }

      String textResult = out.toString("UTF-8")
      int status = channel.exitStatus
      if (Thread.interrupted()) {
        log.debug("Interrupted while waiting for the end of the SSH stream")
        throw new InterruptedIOException()
      }
      return [status, textResult]
    } finally {
      end()
      channel?.disconnect()
    }
  }

/*  List exec(List args, int timeout) {
  ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
  int rv = exec(outputStream, args, timeout)
  String outstr = outputStream.toString("UTF-8")
  [rv, outstr]
}*/
}
