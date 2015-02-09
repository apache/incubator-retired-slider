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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.AccessControlException
import org.apache.hadoop.security.UserGroupInformation

@SuppressWarnings("GroovyOctalInteger")
@Slf4j
@CompileStatic
class FileUploader {
  final Configuration conf
  final UserGroupInformation user

  FileUploader(Configuration conf, UserGroupInformation user) {
    this.conf = conf
    this.user = user
  }

  /**
   * Copy if the file is considered out of date
   * @param src
   * @param destPath
   * @param force
   * @return
   */
  public boolean copyIfOutOfDate(File src, Path destPath, boolean force) {
    if (!src.exists()) {
      throw new FileNotFoundException("Source file $src not found")
    }
    def srcLen = src.length()
    def fs = getFileSystem(destPath)
    boolean toCopy = force
    if (!toCopy) {
      try {
        def status = fs.getFileStatus(destPath)
        toCopy = status.len != srcLen
      } catch (FileNotFoundException fnfe) {
        toCopy = true;
      }
    }
    if (toCopy) {
      log.info("Copying $src to $destPath")
      def dir = destPath.getParent()
      try {
        fs.delete(destPath, true)
        fs.mkdirs(dir, FsPermission.dirDefault)
        return FileUtil.copy(src, fs, destPath, false, conf)
      } catch (AccessControlException ace) {
        log.error("No write access to destination directory $dir" +
                  "Ensure home directory exists and has correct permissions. $ace",
                  ace)
        throw ace
      }
    } else {
      log.debug(
          "Skipping copy as the destination $destPath considered up to date")
      return false;
    }
  }

  public HadoopFS getFileSystem(Path dest) {
    getFileSystem(user, dest)
  }

  public HadoopFS getFileSystem() {
    getFileSystem(user, HadoopFS.getDefaultUri(conf))
  }


  public HadoopFS getFileSystem(
      UserGroupInformation user, final Path path) {
    return getFileSystem(user, path.toUri())

  }

  public HadoopFS getFileSystem(
      UserGroupInformation user, final URI uri) {

    SudoClosure.sudo(user) {
      HadoopFS.get(uri, conf);
    }
  }

  public HadoopFS getFileSystemAsUserName(String username) {

    def user = UserGroupInformation.createRemoteUser(username)
    getFileSystem(user, HadoopFS.getDefaultUri(conf))
  }

  /**
   * Create the home dir. If it can't be created as the user,
   * try to become the user 'hdfs' and try there, setting the
   * user and group after.
   * @return the home dir
   */
  public def mkHomeDir() {
    def fs = fileSystem
    def home = fs.homeDirectory
    if (!fs.exists(home)) {
      try {
        fs.mkdirs(home)
      } catch (AccessControlException ace) {
        log.info("Failed to mkdir $home as $user -impersonating 'hdfs")
        if (UserGroupInformation.securityEnabled) {
          // in a secure cluster, we cannot impersonate HDFS, so rethrow
          throw ace;
        }
        //now create as hdfs
        try {
          attemptToCreateHomeDir("hdfs", home)
        } catch (AccessControlException ace2) {

          log.info("Failed to mkdir $home as $user -impersonating 'hadoop'")
          attemptToCreateHomeDir("hadoop", home)

        }
      }
    }
    return home
  }

  public void attemptToCreateHomeDir(String username, Path home) {
    def privilegedFS = getFileSystemAsUserName(username)
    log.info "Creating home dir $home as user ${user.userName} group ${user.primaryGroupName}"
    privilegedFS.mkdirs(home, new FsPermission((short) 00755))
    privilegedFS.setOwner(home, user.userName, user.primaryGroupName)
  }
}
