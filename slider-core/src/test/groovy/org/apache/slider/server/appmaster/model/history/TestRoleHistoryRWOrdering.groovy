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

import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.slider.common.SliderKeys
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.state.NodeEntry
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.RoleHistory
import org.apache.slider.server.avro.NewerFilesFirst
import org.apache.slider.server.avro.RoleHistoryWriter
import org.junit.Test

import java.util.regex.Matcher
import java.util.regex.Pattern

@Slf4j
class TestRoleHistoryRWOrdering extends BaseMockAppStateTest {

  def paths = pathlist(
      [
          "hdfs://localhost/history-0406c.json",
          "hdfs://localhost/history-5fffa.json",
          "hdfs://localhost/history-0001a.json",
          "hdfs://localhost/history-0001f.json",
      ]
  )
  Path h_0406c = paths[0]
  Path h_5fffa = paths[1]
  Path h_0001a = paths[3]


  List<Path> pathlist(List<String> pathnames) {
    def result = []
    pathnames.each { result << new Path(new URI(it as String)) }
    result
  }

  @Override
  String getTestName() {
    return "TestHistoryRWOrdering"
  }

    
  /**
   * This tests regexp pattern matching. It uses the current time so isn't
   * repeatable -but it does test a wider range of values in the process
   * @throws Throwable
   */
  @Test
  public void testPatternRoundTrip() throws Throwable {
    describe "test pattern matching of names"
    long value=System.currentTimeMillis()
    String name = String.format(SliderKeys.HISTORY_FILENAME_CREATION_PATTERN,value)
    String matchpattern = SliderKeys.HISTORY_FILENAME_MATCH_PATTERN
    Pattern pattern = Pattern.compile(matchpattern)
    Matcher matcher = pattern.matcher(name);
    if (!matcher.find()) {
      throw new Exception("No match for pattern $matchpattern in $name")
    }
  }


  @Test
  public void testWriteSequenceReadData() throws Throwable {
    describe "test that if multiple entries are written, the newest is picked up"
    long time = System.currentTimeMillis();

    RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
    assert !roleHistory.onStart(fs, historyPath)
    String addr = "localhost"
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr)
    NodeEntry ne1 = instance.getOrCreate(0)
    ne1.lastUsed = 0xf00d

    Path history1 = roleHistory.saveHistory(time++)
    Path history2 = roleHistory.saveHistory(time++)
    Path history3 = roleHistory.saveHistory(time++)
    
    //inject a later file with a different name
    sliderFileSystem.cat(new Path(historyPath, "file.json"), true, "hello, world")


    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    
    List<Path> entries = historyWriter.findAllHistoryEntries(
        fs,
        historyPath,
        false)
    assert entries.size() == 3
    assert entries[0] == history3
    assert entries[1] == history2
    assert entries[2] == history1
  }

  @Test
  public void testPathStructure() throws Throwable {
    assert h_5fffa.getName() == "history-5fffa.json"
  }
  
  @Test
  public void testPathnameComparator() throws Throwable {

    def newerName = new NewerFilesFirst()
    
    log.info("$h_5fffa name is ${h_5fffa.getName()}")
    log.info("$h_0406c name is ${h_0406c.getName()}")
    assert  newerName.compare(h_5fffa, h_5fffa) == 0
    assert  newerName.compare(h_5fffa, h_0406c) < 0
    assert  newerName.compare(h_5fffa, h_0001a) < 0
    assert  newerName.compare(h_0001a, h_5fffa) > 0
    
  }
  
  @Test
  public void testPathSort() throws Throwable {
    def paths2 = new ArrayList(paths) 
    RoleHistoryWriter.sortHistoryPaths(paths2)
    assertListEquals(paths2,
                     [
                         paths[1],
                         paths[0],
                         paths[3],
                         paths[2]
                     ])
  }
}
