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
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.BeforeClass;
import org.junit.Test

@CompileStatic
@Slf4j
public class AMFailuresIT extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "am-failures-it"
  public static final String TEST_REMOTE_SSH_KEY = "test.remote.ssh.key"
  public static final String VAGRANT_CWD = "vagrant.current.working.dir"
  File sshkey

  @BeforeClass
  public static void setupAMTests() {
    assumeAmFailureTestsEnabled()
  }

  @After
  public void destroyCluster() {
    cleanup(APPLICATION_NAME)
  }

  @Test
  public void testAMKilledWithStateAMStartedAgentsStarted() throws Throwable {
    cleanup(APPLICATION_NAME)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createTemplatedSliderApplication(
        APPLICATION_NAME, APP_TEMPLATE, APP_RESOURCE,
        [],
        launchReportFile)
    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    expectContainerRequestedCountReached(APPLICATION_NAME, COMMAND_LOGGER, 1,
        CONTAINER_LAUNCH_TIMEOUT)

    def cd = assertContainersLive(APPLICATION_NAME, COMMAND_LOGGER, 1)
    def loggerInstances = cd.instances[COMMAND_LOGGER]
    assert loggerInstances.size() == 1

    def loggerStats = cd.statistics[COMMAND_LOGGER]

    assert loggerStats["containers.requested"] == 1
    assert loggerStats["containers.live"] == 1

    // Now kill the AM
    log.info("Killing AM now ...")
    killAmAndWaitForRestart(APPLICATION_NAME, appId)

    // There should be exactly 1 live logger container
    def cd2 = assertContainersLive(APPLICATION_NAME, COMMAND_LOGGER, 1)

    // No new containers should be requested for the agents
    def restartedStats = cd2.statistics[COMMAND_LOGGER]
    assert restartedStats["containers.live"] == 1

    assert 0==restartedStats["containers.requested"],
        'No new agent containers should be requested'
    assert lookupYarnAppState(appId) == YarnApplicationState.RUNNING 
  }

  protected void killAMUsingVagrantShell() {
    String hostname = SLIDER_CONFIG.get(YarnConfiguration.RM_ADDRESS).split(":")[0]
    assert hostname
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
    assert hostname
    assert user

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
