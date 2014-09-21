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

package org.apache.slider.funtest.lifecycle

import com.jcraft.jsch.Session
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.bigtop.itest.shell.Shell
import org.apache.chaos.remote.RemoteServer
import org.apache.chaos.remote.SshCommands
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Test

@CompileStatic
@Slf4j
public class AMFailuresIT extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "am-started-agents-started"
  public static final String TEST_REMOTE_SSH_KEY = "test.remote.ssh.key"
  public static final String VAGRANT_CWD = "vagrant.current.working.dir"
  File sshkey

  @After
  public void destroyCluster() {
    cleanup(APPLICATION_NAME)
  }

  @Test
  public void testAMKilledWithStateAMStartedAgentsStarted() throws Throwable {
    cleanup(APPLICATION_NAME)
    SliderShell shell = slider(EXIT_SUCCESS,
        [
            ACTION_CREATE, APPLICATION_NAME,
            ARG_TEMPLATE, APP_TEMPLATE,
            ARG_RESOURCES, APP_RESOURCE
        ])
    logShell(shell)

    ensureApplicationIsUp(APPLICATION_NAME)
    repeatUntilTrue(this.&hasContainerCountExceeded, 15, 1000 * 10, ['arg1': '1']);
    // Wait for 20 secs for AM and agent to both reach STARTED state
    sleep(1000 * 20)

    shell = slider(EXIT_SUCCESS,
        [
            ACTION_STATUS,
            APPLICATION_NAME])
    assertComponentCount(COMMAND_LOGGER, 1, shell)
    String live = findLineEntryValue(shell, ["statistics", COMMAND_LOGGER, "containers.live"] as String[])
    assert live != null && live.isInteger() && live.toInteger() == 1,
        'At least 1 container must be live now'

    assert isApplicationInState("RUNNING", APPLICATION_NAME), 'App is not running.'
    assertSuccess(shell)

    // Now kill the AM
    log.info("Killing AM now ...")
//    killAMUsingJsch()
//    killAMUsingAmSuicide()
    killAMUsingVagrantShell()

    // Check that the application is not running (and is in ACCEPTED state)
    assert isApplicationInState("ACCEPTED", APPLICATION_NAME), 
      'App should be in ACCEPTED state (since AM got killed)'
    log.info("After AM KILL: application {} is in ACCEPTED state", APPLICATION_NAME)

    // Wait until AM comes back up and verify container count again
    ensureApplicationIsUp(APPLICATION_NAME)

    // There should be exactly 1 live container
    shell = slider(EXIT_SUCCESS,
      [
          ACTION_STATUS,
          APPLICATION_NAME])

    live = findLineEntryValue(shell, ["statistics", COMMAND_LOGGER, "containers.live"] as String[])
    assert live != null && live.isInteger() && live.toInteger() == 1,
        'At least 1 container must be live now'
    log.info("After AM KILL: agent container is still live")

    // No new containers should be requested for the agents
    String requested = findLineEntryValue(shell, ["statistics", COMMAND_LOGGER, "containers.requested"] as String[])
    assert requested != null && requested.isInteger() && requested.toInteger() == 0,
        'No new agent containers should be requested'
    log.info("After AM KILL: no new agent containers were requested")

    assert isApplicationInState("RUNNING", APPLICATION_NAME), 'App is not running.'
    assertSuccess(shell)
  }

  boolean hasContainerCountExceeded(Map<String, String> args) {
    int expectedCount = args['arg1'].toInteger();
    SliderShell shell = slider(EXIT_SUCCESS,
        [
            ACTION_STATUS,
            APPLICATION_NAME])

    String requested = findLineEntryValue(
        shell, ["statistics", COMMAND_LOGGER, "containers.requested"] as String[])
    if (requested != null && requested.isInteger() && requested.toInteger() >= expectedCount) {
      return true
    }

    return false
  }

  protected void killAMUsingAmSuicide() {
    SliderShell shell = slider(EXIT_SUCCESS,
      [
          ACTION_AM_SUICIDE,
          ARG_MESSAGE, "testAMRestart",
          APPLICATION_NAME])
    logShell(shell)
    assertSuccess(shell)
  }

  protected void killAMUsingVagrantShell() {
    String hostname = SLIDER_CONFIG.get(YarnConfiguration.RM_ADDRESS).split(":")[0]
    assert hostname != null && !hostname.isEmpty()
    String vagrantVmName = hostname.split("\\.")[0]

    String vagrantCwd = sysprop(VAGRANT_CWD)
    log.info("VAGRANT_CWD = {}", vagrantCwd)
    File dirCheck = new File(vagrantCwd)
    assert dirCheck.exists(), "Please set $VAGRANT_CWD to the directory which contains the Vagrantfile"

    String cmd = "export VAGRANT_CWD=$vagrantCwd; /usr/bin/vagrant ssh -c \"sudo -u root runuser -l yarn " +
      "-c \\\"ps -ef | grep SliderAppMaster | egrep -v 'grep|bash' | sed 's/^[a-z]* *\\([^ ]*\\) *.*/\\1/' " + 
      "| xargs kill -9\\\"\" $vagrantVmName 2>/dev/null"
    log.info("Vagrant Shell Command = {}", cmd)

    Shell vagrantShell = new Shell('/bin/bash -s')
    vagrantShell.exec(cmd)
  }

  protected void killAMUsingJsch() {
    String hostname = SLIDER_CONFIG.get(YarnConfiguration.RM_ADDRESS).split(":")[0]
    String user = UserGroupInformation.currentUser
    assert hostname != null && !hostname.isEmpty()
    assert user != null && !user.isEmpty()

    bindSSHKey()
    RemoteServer remoteServer = new RemoteServer(
        host: hostname,
        username: user,
        publicKeyFile: sshkey)
    Session session = remoteServer.connect()
    SshCommands cmds = new SshCommands(session)
//    def (String rv, String out) = cmds.command([
//      "ps -ef | grep SliderAppMaster | egrep -v 'grep|bash' | xargs kill -9"
//    ])
//    assert rv == 0
//    log.info("Kill cmd output", out)
  }

  /**
   * Bind to the SSH key -assert that the file actually exists
   */
  protected void bindSSHKey() {
    sshkey = new File(sysprop("user.home"), ".ssh/id_rsa")
    sshkey = new File(sysprop(TEST_REMOTE_SSH_KEY), sshkey.toString())
    assert sshkey.exists()
  }
}
