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

package org.apache.slider.core.persist

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.tools.CoreFileSystem
import org.apache.slider.test.YarnMiniClusterTestBase
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

@CompileStatic
@Slf4j
public class TestConfPersisterLocksHDFS extends YarnMiniClusterTestBase {
  static MiniDFSCluster hdfs
  static private YarnConfiguration conf = new YarnConfiguration()
  static CoreFileSystem coreFileSystem
  static URI fsURI
  static HadoopFS dfsClient

  TestConfPersisterLocksHDFS() {
    
  }

  @BeforeClass
  public static void createCluster() {
    hdfs = buildMiniHDFSCluster(
        "TestConfPersister",
        conf)

    fsURI = new URI(buildFsDefaultName(hdfs))
    dfsClient = HadoopFS.get(fsURI, conf);
    coreFileSystem = new CoreFileSystem(dfsClient, conf)
  }
  
  @AfterClass
  public static void destroyCluster() {
    hdfs?.shutdown()
    hdfs = null;
  }

  /**
   * Create the persister. This also creates the destination directory
   * @param name name of cluster
   * @return a conf persister
   */
  public ConfPersister createPersister(String name) {
    def path = coreFileSystem.buildClusterDirPath(name);
    ConfPersister persister = new ConfPersister(
        coreFileSystem,
        path)
    coreFileSystem.getFileSystem().mkdirs(path)
    return persister
  }      

  @Test
  public void testReleaseNonexistentWritelock() throws Exception {

    ConfPersister persister = createPersister("testReleaseNonexistentWritelock")
    assert !persister.releaseWritelock();
  }


  @Test
  public void testAcqRelWriteLock() throws Throwable {
    ConfPersister persister = createPersister("testAcqRelWriteLock")
    persister.acquireWritelock();
    assert persister.releaseWritelock();
    assert !persister.releaseWritelock()
  }

  @Test
  public void testSecondWriteLockAcqFails() throws Throwable {
    ConfPersister persister = createPersister("testSecondWriteLockAcqFails")
    persister.acquireWritelock();
    try {
      persister.acquireWritelock();
      fail "write lock acquired twice"
    } catch (LockAcquireFailedException lafe) {
      //expected
      assert lafe.path.toString().endsWith(Filenames.WRITELOCK)
    }
    assert persister.releaseWritelock();
    
    //now we can ask for it
    persister.acquireWritelock();
  }

  @Test
  public void testReleaseNonexistentReadlockOwner() throws Exception {
    ConfPersister persister = createPersister("testReleaseNonexistentReadlock")
    assert !persister.releaseReadlock(true);
  }
  
  @Test
  public void testReleaseNonexistentReadlock() throws Exception {
    ConfPersister persister = createPersister("testReleaseNonexistentReadlock")
    assert !persister.releaseReadlock(false)
  }
  
  @Test
  public void testAcqRelReadlock() throws Exception {
    ConfPersister persister = createPersister("testAcqRelReadlock")
    assert persister.acquireReadLock();
    assert persister.readLockExists();

    assert !persister.releaseReadlock(false);
    assert persister.readLockExists();
    assert persister.releaseReadlock(true);
  }

  @Test
  public void testAcqAcqRelReadlock() throws Exception {
    ConfPersister persister = createPersister("testAcqRelReadlock")
    assert persister.acquireReadLock();
    assert persister.readLockExists();
    assert !persister.acquireReadLock();
    assert persister.readLockExists();

    assert !persister.releaseReadlock(false);
    assert persister.readLockExists();
    assert persister.releaseReadlock(true);
    assert !persister.readLockExists();
  }
  
  @Test
  public void testAcqAcqRelReadlockOtherOrderOfRelease() throws Exception {
    ConfPersister persister = createPersister("testAcqRelReadlock")
    assert persister.acquireReadLock();
    assert persister.readLockExists();
    assert !persister.acquireReadLock();
    assert persister.readLockExists();

    assert persister.releaseReadlock(true);
    assert !persister.readLockExists();
    assert !persister.releaseReadlock(false)

  }

  
  @Test
  public void testNoReadlockWhenWriteHeld() throws Throwable {
    ConfPersister persister = createPersister("testNoReadlockWhenWriteHeld")
    persister.acquireWritelock();
    try {
      persister.acquireReadLock();
      fail "read lock acquired"
    } catch (LockAcquireFailedException lafe) {
      //expected
      assertWritelockBlocked(lafe)
    }
    assert persister.releaseWritelock();
    assert !persister.writelockExists();
    
    //now we can ask for it
    persister.acquireReadLock();
  }

  public void assertWritelockBlocked(LockAcquireFailedException lafe) {
    assert lafe.path.toString().endsWith(Filenames.WRITELOCK)
  }

  public void assertReadlockBlocked(LockAcquireFailedException lafe) {
    assert lafe.path.toString().endsWith(Filenames.READLOCK)
  }

  @Test
  public void testNoWritelockWhenReadHeld() throws Throwable {
    ConfPersister persister = createPersister("testNoWritelockWhenReadHeld")
    assert persister.acquireReadLock();
    try {
      persister.acquireWritelock();
      fail "write lock acquired"
    } catch (LockAcquireFailedException lafe) {
      //expected
      assertReadlockBlocked(lafe)
    }
    assert persister.releaseReadlock(true);
    
    //now we can ask for it
    persister.acquireWritelock();
  }

  
}
