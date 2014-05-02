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
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.tools.CoreFileSystem
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.conf.ExampleConfResources
import org.apache.slider.test.YarnMiniClusterTestBase
import org.junit.BeforeClass
import org.junit.Test

@CompileStatic
@Slf4j
public class TestConfPersisterReadWrite extends YarnMiniClusterTestBase {
  static private YarnConfiguration conf = new YarnConfiguration()
  static CoreFileSystem coreFileSystem
  static URI fsURI
  static HadoopFS dfsClient
  static final JsonSerDeser<ConfTree> confTreeJsonSerDeser =
      new JsonSerDeser<ConfTree>(ConfTree)
  AggregateConf aggregateConf = ExampleConfResources.loadExampleAggregateResource()


  TestConfPersisterReadWrite() {
    
  }

  @BeforeClass
  public static void createCluster() {
    fsURI = new URI(buildFsDefaultName(null))
    dfsClient = HadoopFS.get(fsURI, conf);
    coreFileSystem = new CoreFileSystem(dfsClient, conf)
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
  public void testSaveLoadEmptyConf() throws Throwable {
    AggregateConf aggregateConf = new AggregateConf()

    def persister = createPersister("testSaveLoad")
    persister.save(aggregateConf, null)
    AggregateConf loaded = new AggregateConf()
    persister.load(loaded)
    loaded.validate()
  }
 
  
  @Test
  public void testSaveLoadTestConf() throws Throwable {
    def persister = createPersister("testSaveLoadTestConf")
    persister.save(aggregateConf, null)
    AggregateConf loaded = new AggregateConf()
    persister.load(loaded)
    loaded.validate()
  }
 
  
    
  @Test
  public void testSaveLoadTestConfResolveAndCheck() throws Throwable {
    def appConfOperations = aggregateConf.getAppConfOperations()
    appConfOperations.getMandatoryComponent("master")["PATH"]="."
    def persister = createPersister("testSaveLoadTestConf")
    persister.save(aggregateConf, null)
    AggregateConf loaded = new AggregateConf()
    persister.load(loaded)
    loaded.validate()
    loaded.resolve();
    def resources = loaded.getResourceOperations()
    def master = resources.getMandatoryComponent("master")
    assert master["yarn.memory"] == "1024"

    def appConfOperations2 = loaded.getAppConfOperations()
    assert appConfOperations2.getMandatoryComponent("master")["PATH"] == "."

  } 
  
  @Test
  public void testSaveFailsIfWritelocked() throws Throwable {
    def persister = createPersister("testSaveFailsIfWritelocked")
    persister.releaseWritelock()
    persister.acquireWritelock()
    try {
      expectSaveToFailOnLock(persister, aggregateConf)
    } finally {
      persister.releaseWritelock()
    }
  }

  @Test
  public void testSaveFailsIfReadlocked() throws Throwable {
    def persister = createPersister("testSaveFailsIfReadlocked")
    persister.releaseWritelock()
    persister.acquireReadLock()
    try {
      expectSaveToFailOnLock(persister, aggregateConf)
    } finally {
      persister.releaseReadlock(true)
    }
  }
    
  @Test
  public void testLoadFailsIfWritelocked() throws Throwable {
    def persister = createPersister("testLoadFailsIfWritelocked")
    persister.acquireWritelock()
    try {
      expectLoadToFailOnLock(persister, aggregateConf)
    } finally {
      persister.releaseWritelock()
    }
  }
    
  @Test
  public void testLoadFailsIfDestDoesNotExist() throws Throwable {
    def persister = createPersister("testLoadFailsIfDestDoesNotExist")
    try {
      persister.load(aggregateConf)
      fail "expected save to fail to find a file"
    } catch (FileNotFoundException e) {
      //expected
    }
  }

  @Test
  public void testLoadSucceedsIfReadlocked() throws Throwable {
    def persister = createPersister("testLoadSucceedsIfReadlocked")
    persister.releaseReadlock(true)
    try {
      persister.save(aggregateConf, null)
      persister.acquireReadLock()
      AggregateConf loaded = new AggregateConf()
      persister.load(loaded)
      loaded.validate()
      loaded.resolve()
    } finally {
      persister.releaseReadlock(true)
    }
  }
  
  public void expectSaveToFailOnLock(
      ConfPersister persister,
      AggregateConf aggregateConf) {
    try {
      persister.save(aggregateConf, null)
      fail "expected save to fail to get a lock"
    } catch (LockAcquireFailedException lafe) {
      //expected
    }
  }
  
  
  public void expectLoadToFailOnLock(
      ConfPersister persister,
      AggregateConf aggregateConf) {
    try {
      persister.load(aggregateConf)
      fail "expected save to fail to get a lock"
    } catch (LockAcquireFailedException lafe) {
      //expected
    }
  }


}
