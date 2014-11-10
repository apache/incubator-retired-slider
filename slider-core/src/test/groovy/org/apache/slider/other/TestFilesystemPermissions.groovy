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

package org.apache.slider.other

import groovy.io.FileType
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.AbstractFileSystem
import org.apache.hadoop.fs.FileContext
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.local.LocalFs
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.DiskChecker
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService
import org.apache.slider.test.YarnMiniClusterTestBase
import org.junit.After
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This test class exists to look at permissions of the filesystem, especially
 * that created by Mini YARN clusters. On some windows jenkins machines, 
 * YARN actions were failing as the directories had the wrong permissions 
 * (i.e. too lax)
 */
@CompileStatic
@Slf4j
class TestFilesystemPermissions extends YarnMiniClusterTestBase {

  static final Logger LOG = LoggerFactory.getLogger(TestFilesystemPermissions);

  List<File> filesToDelete = []

  @After
  public void deleteFiles() {
    filesToDelete.each { File f ->
      FileUtil.fullyDelete(f, true)
    }
  }

  @Test
  public void testJavaFSOperations() throws Throwable {
    assertNativeLibrariesPresent();
    File subdir = testDir()
    subdir.mkdir()
    assert subdir.isDirectory()
    assert FileUtil.canRead(subdir)
    assert FileUtil.canWrite(subdir)
    assert FileUtil.canExecute(subdir)
  }

  @Test
  public void testDiskCheckerOperations() throws Throwable {
    assertNativeLibrariesPresent();
    File subdir = testDir()
    subdir.mkdir()
    DiskChecker checker = new DiskChecker()
    checker.checkDir(subdir)
  }

  @Test
  public void testDiskCheckerMkdir() throws Throwable {
    assertNativeLibrariesPresent();
    File subdir = testDir()
    subdir.mkdirs()
    DiskChecker checker = new DiskChecker()
    checker.checkDir(subdir)
  }

  /**
   * Get a test dir for this method; one that will be deleted on teardown
   * @return a filename unique to this test method
   */
  File testDir() {
    File parent = new File("target/testfspermissions")
    parent.mkdir()
    File testdir = new File(parent, methodName.methodName)
    filesToDelete << testdir
    return testdir;
  }

  
  @Test
  public void testPermsMap() throws Throwable {
    def dir = testDir()
    def diruri = dir.toURI().toString()
    def lfs = createLocalFS(dir, configuration)
    getLocalDirsPathPermissionsMap(lfs, diruri)
  }

  @Test
  public void testInitLocaldir() throws Throwable {
    def dir = testDir()
    def diruri = dir.toURI().toString()
    def lfs = createLocalFS(dir, configuration)
    initializeLocalDir(lfs, diruri)
    def localDirs = getInitializedLocalDirs(lfs, [diruri])
    assert localDirs.size() ==1
  }


  @Test
  public void testValidateMiniclusterPerms() throws Throwable {
    def numLocal = 1
    def cluster = createMiniCluster("", configuration, 1, numLocal, 1, false)
    def workDir = miniCluster.getTestWorkDir()
    List<File> localdirs = [];
    workDir.eachDir { File file ->
      if (file.absolutePath.contains("-local")) {
        // local dir
        localdirs << file
      }
    }
    assert localdirs.size() == numLocal
    def lfs = createLocalFS(workDir, configuration)
    localdirs.each { File file -> 
      checkLocalDir(lfs, file.toURI().toString())
    }
  }
  
  FileContext createLocalFS(File dir, Configuration conf) {
    return FileContext.getFileContext(dir.toURI(), conf)
  }
  
  /**
   * extracted from ResourceLocalizationService
   * @param lfs
   * @param localDir
   * @return perms map
   * @see ResourceLocalizationService
   */
  private Map<Path, FsPermission> getLocalDirsPathPermissionsMap(
      FileContext lfs,
      String localDir) {
    Map<Path, FsPermission> localDirPathFsPermissionsMap = new HashMap<Path, FsPermission>();

    FsPermission defaultPermission =
        FsPermission.getDirDefault().applyUMask(lfs.getUMask());
    FsPermission nmPrivatePermission =
        ResourceLocalizationService.NM_PRIVATE_PERM.applyUMask(lfs.getUMask());

    Path userDir = new Path(localDir, ContainerLocalizer.USERCACHE);
    Path fileDir = new Path(localDir, ContainerLocalizer.FILECACHE);
    Path sysDir = new Path(
        localDir,
        ResourceLocalizationService.NM_PRIVATE_DIR);

    localDirPathFsPermissionsMap.put(userDir, defaultPermission);
    localDirPathFsPermissionsMap.put(fileDir, defaultPermission);
    localDirPathFsPermissionsMap.put(sysDir, nmPrivatePermission);
    return localDirPathFsPermissionsMap;
  }

  private boolean checkLocalDir(FileContext lfs, String localDir) {

    Map<Path, FsPermission> pathPermissionMap =
        getLocalDirsPathPermissionsMap(lfs, localDir);

    for (Map.Entry<Path, FsPermission> entry : pathPermissionMap.entrySet()) {
      FileStatus status;
      status = lfs.getFileStatus(entry.getKey());

      if (!status.getPermission().equals(entry.getValue())) {
        String msg =
            "Permissions incorrectly set for dir " + entry.getKey() +
        ", should be " + entry.getValue() + ", actual value = " +
        status.getPermission();
        throw new YarnRuntimeException(msg);
      }
    }
    return true;
  }


  private void initializeLocalDir(FileContext lfs, String localDir) {

    Map<Path, FsPermission> pathPermissionMap =
        getLocalDirsPathPermissionsMap(lfs, localDir);
    for (Map.Entry<Path, FsPermission> entry : pathPermissionMap.entrySet()) {
      FileStatus status;
      try {
        status = lfs.getFileStatus(entry.getKey());
      }
      catch (FileNotFoundException fs) {
        status = null;
      }

      if (status == null) {
        lfs.mkdir(entry.getKey(), entry.getValue(), true);
        status = lfs.getFileStatus(entry.getKey());
      }
      FsPermission perms = status.getPermission();
      if (!perms.equals(entry.getValue())) {
        lfs.setPermission(entry.getKey(), entry.getValue());
      }
    }
  }

  synchronized private List<String> getInitializedLocalDirs(FileContext lfs,
      List<String> dirs) {
    List<String> checkFailedDirs = new ArrayList<String>();
    for (String dir : dirs) {
      try {
        checkLocalDir(lfs, dir);
      } catch (YarnRuntimeException e) {
        checkFailedDirs.add(dir);
      }
    }
    for (String dir : checkFailedDirs) {
      LOG.info("Attempting to initialize " + dir);
      initializeLocalDir(lfs, dir);
      checkLocalDir(lfs, dir);
    }
    return dirs;
  }


  private void createDir(FileContext localFs, Path dir, FsPermission perm)
  throws IOException {
    if (dir == null) {
      return;
    }
    try {
      localFs.getFileStatus(dir);
    } catch (FileNotFoundException e) {
      createDir(localFs, dir.getParent(), perm);
      localFs.mkdir(dir, perm, false);
      if (!perm.equals(perm.applyUMask(localFs.getUMask()))) {
        localFs.setPermission(dir, perm);
      }
    }
  }
}
