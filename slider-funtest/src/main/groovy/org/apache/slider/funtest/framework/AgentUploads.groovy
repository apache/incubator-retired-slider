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

import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.fs.FileSystem as HadoopFS

@Slf4j
class AgentUploads implements FuntestProperties {
  final Configuration conf
  public final FileUploader uploader
  public final HadoopFS clusterFS
  public final Path homeDir

  AgentUploads(Configuration conf) {
    this.conf = conf
    uploader = new FileUploader(conf, UserGroupInformation.currentUser)
    clusterFS = uploader.fileSystem
    homeDir = clusterFS.homeDirectory
  }

  /**
   * Upload agent-related files
   * @param tarballDir
   * @param force
   * @return
   */
  def uploadAgentFiles(File tarballDir, boolean force) {
    def localAgentTar = new File(tarballDir, AGENT_SLIDER_GZ_IN_SLIDER_TAR)
    def agentTarballPath = new Path(
        homeDir,
        AGENT_TAR_FILENAME)

    //create the home dir or fail
    uploader.mkHomeDir()
    // Upload the agent tarball
    uploader.copyIfOutOfDate(localAgentTar, agentTarballPath, force)

    File localAgentIni = new File(tarballDir, AGENT_INI_IN_SLIDER_TAR)
    // Upload the agent.ini
    def agentIniPath = new Path(homeDir, AGENT_INI)
    uploader.copyIfOutOfDate(localAgentIni, agentIniPath, force)
    return [agentTarballPath, agentIniPath]
  }

}
