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

package org.apache.slider.server.appmaster.model.history

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.slider.providers.PlacementPolicy
import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.state.NodeEntry
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.RoleHistory
import org.apache.slider.server.avro.LoadedRoleHistory
import org.apache.slider.server.avro.RoleHistoryWriter
import org.junit.Test

@Slf4j
@CompileStatic
class TestRoleHistoryRW extends BaseMockAppStateTest {

  static long time = System.currentTimeMillis();
  public static final String HISTORY_V1_6_ROLE = "org/apache/slider/server/avro/history-v01-6-role.json"
  public static final String HISTORY_V1_3_ROLE = "org/apache/slider/server/avro/history-v01-3-role.json"


  static final ProviderRole PROVIDER_ROLE3 = new ProviderRole(
      "role3",
      3,
      PlacementPolicy.STRICT,
      3,
      3)

  @Override
  String getTestName() {
    return "TestHistoryRW"
  }

  @Test
  public void testWriteReadEmpty() throws Throwable {
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    roleHistory.onStart(fs, historyPath)
    Path history = roleHistory.saveHistory(time++)
    assert fs.isFile(history)
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    historyWriter.read(fs, history)
  }
  
  @Test
  public void testWriteReadData() throws Throwable {
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    assert !roleHistory.onStart(fs, historyPath)
    String addr = "localhost"
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry ne1 = instance.getOrCreate(0)
    ne1.lastUsed = 0xf00d

    Path history = roleHistory.saveHistory(time++)
    assert fs.isFile(history)
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    RoleHistory rh2 = new RoleHistory(MockFactory.ROLES)


    def loadedRoleHistory = historyWriter.read(fs, history)
    assert 0 < loadedRoleHistory.size()
    rh2.rebuild(loadedRoleHistory)
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr)
    assert ni2 != null
    NodeEntry ne2 = ni2.get(0)
    assert ne2 !=null
    assert ne2.lastUsed == ne1.lastUsed
  }
    
  @Test
  public void testWriteReadActiveData() throws Throwable {
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    roleHistory.onStart(fs, historyPath)
    String addr = "localhost"
    String addr2 = "rack1server5"
    NodeInstance localhost = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry orig1 = localhost.getOrCreate(0)
    orig1.lastUsed = 0x10
    NodeInstance rack1server5 = roleHistory.getOrCreateNodeInstance(addr2)
    NodeEntry orig2 = rack1server5.getOrCreate(1)
    orig2.live = 3
    assert !orig2.available
    NodeEntry orig3 = localhost.getOrCreate(1)
    orig3.lastUsed = 0x20
    orig3.live = 1
    assert !orig3.available
    orig3.release()
    assert orig3.available
    roleHistory.dump()

    long savetime = 0x0001000
    Path history = roleHistory.saveHistory(savetime)
    assert fs.isFile(history)
    describe("Loaded")
    log.info("testWriteReadActiveData in $history")
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    RoleHistory rh2 = new RoleHistory(MockFactory.ROLES)
    def loadedRoleHistory = historyWriter.read(fs, history)
    assert 3 == loadedRoleHistory.size()
    rh2.rebuild(loadedRoleHistory)
    rh2.dump()

    assert rh2.clusterSize == 2;
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr)
    assert ni2 != null
    NodeEntry loadedNE = ni2.get(0)
    assert loadedNE.lastUsed == orig1.lastUsed
    NodeInstance ni2b = rh2.getExistingNodeInstance(addr2)
    assert ni2b != null
    NodeEntry loadedNE2 = ni2b.get(1)
    assert loadedNE2 != null
    assert loadedNE2.lastUsed == savetime
    assert rh2.thawedDataTime == savetime

    // now start it
    rh2.buildAvailableNodeLists();
    describe("starting")
    rh2.dump();
    List<NodeInstance> available0 = rh2.cloneAvailableList(0)
    assert available0.size() == 1

    NodeInstance entry = available0.get(0)
    assert entry.hostname == "localhost"
    assert entry == localhost
    List<NodeInstance> available1 = rh2.cloneAvailableList(1)
    assert available1.size() == 2
    //and verify that even if last used was set, the save time is picked up
    assert entry.get(1).lastUsed == roleHistory.saveTime

  }

  @Test
  public void testWriteThaw() throws Throwable {
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    assert !roleHistory.onStart(fs, historyPath)
    String addr = "localhost"
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry ne1 = instance.getOrCreate(0)
    ne1.lastUsed = 0xf00d

    Path history = roleHistory.saveHistory(time++)
    long savetime =roleHistory.saveTime;
    assert fs.isFile(history)
    RoleHistory rh2 = new RoleHistory(MockFactory.ROLES)
    assert rh2.onStart(fs, historyPath)
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr)
    assert ni2 != null
    NodeEntry ne2 = ni2.get(0)
    assert ne2 != null
    assert ne2.lastUsed == ne1.lastUsed
    assert rh2.thawedDataTime == savetime
  }


  @Test
  public void testPurgeOlderEntries() throws Throwable {
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    time = 1;
    Path file1 = touch(historyWriter, time++)
    Path file2 = touch(historyWriter, time++)
    Path file3 = touch(historyWriter, time++)
    Path file4 = touch(historyWriter, time++)
    Path file5 = touch(historyWriter, time++)
    Path file6 = touch(historyWriter, time++)
    
    assert historyWriter.purgeOlderHistoryEntries(fs, file1) == 0
    assert historyWriter.purgeOlderHistoryEntries(fs, file2) == 1
    assert historyWriter.purgeOlderHistoryEntries(fs, file2) == 0
    assert historyWriter.purgeOlderHistoryEntries(fs, file5) == 3
    assert historyWriter.purgeOlderHistoryEntries(fs, file6) == 1
    try {
      // make an impossible assertion that will fail if the method
      // actually completes
      assert -1 == historyWriter.purgeOlderHistoryEntries(fs, file1) 
    } catch (FileNotFoundException ignored) {
      //  expected
    }
    
  }
  
  public Path touch(RoleHistoryWriter historyWriter, long time){
    Path path = historyWriter.createHistoryFilename(historyPath, time);
    FSDataOutputStream out = fs.create(path);
    out.close()
    return path
  }

  @Test
  public void testSkipEmptyFileOnRead() throws Throwable {
    describe "verify that empty histories are skipped on read; old histories purged"
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    roleHistory.onStart(fs, historyPath)
    time = 0
    Path oldhistory = roleHistory.saveHistory(time++)

    String addr = "localhost"
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry ne1 = instance.getOrCreate(0)
    ne1.lastUsed = 0xf00d

    Path goodhistory = roleHistory.saveHistory(time++)

    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    Path touched = touch(historyWriter, time++)

    RoleHistory rh2 = new RoleHistory(MockFactory.ROLES)
    assert rh2.onStart(fs, historyPath)
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr)
    assert ni2 != null

    //and assert the older file got purged
    assert !fs.exists(oldhistory)
    assert fs.exists(goodhistory)
    assert fs.exists(touched )
  }

  @Test
  public void testSkipBrokenFileOnRead() throws Throwable {
    describe "verify that empty histories are skipped on read; old histories purged"
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    roleHistory.onStart(fs, historyPath)
    time = 0
    Path oldhistory = roleHistory.saveHistory(time++)

    String addr = "localhost"
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry ne1 = instance.getOrCreate(0)
    ne1.lastUsed = 0xf00d

    Path goodhistory = roleHistory.saveHistory(time++)

    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    Path badfile = historyWriter.createHistoryFilename(historyPath, time++)
    FSDataOutputStream out = fs.create(badfile)
    out.writeBytes("{broken:true}")
    out.close()

    RoleHistory rh2 = new RoleHistory(MockFactory.ROLES)
    describe("IGNORE STACK TRACE BELOW")

    assert rh2.onStart(fs, historyPath)
    
    describe( "IGNORE STACK TRACE ABOVE")
    NodeInstance ni2 = rh2.getExistingNodeInstance(addr)
    assert ni2 != null

    //and assert the older file got purged
    assert !fs.exists(oldhistory)
    assert fs.exists(goodhistory)
    assert fs.exists(badfile )
  }

  /**
   * Test that a v1 JSON file can be read. Here the number of roles
   * matches the current state.
   * @throws Throwable
   */
  @Test
  public void testReloadDataV1_3_role() throws Throwable {
    String source = HISTORY_V1_3_ROLE
    RoleHistoryWriter writer = new RoleHistoryWriter()

    def loadedRoleHistory = writer.read(source)
    assert 4 == loadedRoleHistory.size()
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    assert 0 == roleHistory.rebuild(loadedRoleHistory)
  }

  /**
   * Test that a v1 JSON file can be read. Here more roles than expected
   * @throws Throwable
   */
  @Test
  public void testReloadDataV1_6_role() throws Throwable {
    String source = HISTORY_V1_6_ROLE
    RoleHistoryWriter writer = new RoleHistoryWriter()

    def loadedRoleHistory = writer.read(source)
    assert 6 == loadedRoleHistory.size()
    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    assert 3 == roleHistory.rebuild(loadedRoleHistory)
  }

  /**
   * Test that a v1 JSON file can be read. Here the number of roles
   * is less than the current state.
   * @throws Throwable
   */
  @Test
  public void testReload_less_roles() throws Throwable {
    String source = HISTORY_V1_3_ROLE
    RoleHistoryWriter writer = new RoleHistoryWriter()

    def loadedRoleHistory = writer.read(source)
    assert 4 == loadedRoleHistory.size()
    def expandedRoles = new ArrayList(MockFactory.ROLES)
    expandedRoles << PROVIDER_ROLE3
    RoleHistory roleHistory = new RoleHistory(expandedRoles)
    assert 0 == roleHistory.rebuild(loadedRoleHistory)
  }

}
