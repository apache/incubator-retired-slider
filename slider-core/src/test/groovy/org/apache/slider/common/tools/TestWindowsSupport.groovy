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

package org.apache.slider.common.tools

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.ChecksumFileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.util.Shell
import org.apache.slider.providers.agent.AgentUtils
import org.apache.slider.test.SliderTestUtils
import org.junit.Test

import java.util.regex.Pattern

@CompileStatic
@Slf4j
class TestWindowsSupport extends SliderTestUtils {

  private static final Pattern hasDriveLetterSpecifier =
      Pattern.compile("^/?[a-zA-Z]:");
  public static
  final String windowsFile = "C:\\Users\\Administrator\\AppData\\Local\\Temp\\junit3180177850133852404\\testpkg\\appdef_1.zip"


  private static boolean hasWindowsDrive(String path) {
    return hasDriveLetterSpecifier.matcher(path).find();
  }

  private static int startPositionWithoutWindowsDrive(String path) {
    if (hasWindowsDrive(path)) {
      return path.charAt(0) == '/' ? 3 : 2;
    } else {
      return 0;
    }
  }

  @Test
  public void testHasWindowsDrive() throws Throwable {
    assert hasWindowsDrive(windowsFile)
  }

  @Test
  public void testStartPosition() throws Throwable {
    assert 2 == startPositionWithoutWindowsDrive(windowsFile)
  }
  
  @Test
  public void testPathHandling() throws Throwable {
    assume(Shell.WINDOWS, "not windows")
    
    Path path = new Path(windowsFile);
    def uri = path.toUri()
//    assert "file" == uri.scheme 
    assert uri.authority == null;

    Configuration conf = new Configuration()

    def localfs = HadoopFS.get(uri, conf)
    assert localfs instanceof ChecksumFileSystem
    try {
      def stat = localfs.getFileStatus(path)
      fail("expected an exception, got $stat")
    } catch (FileNotFoundException fnfe) {
      // expected
    }

    try {
      FSDataInputStream appStream = localfs.open(path);
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

  @Test
  public void testSliderFS() throws Throwable {
    assume(Shell.WINDOWS, "not windows")

    SliderFileSystem sfs = new SliderFileSystem(new Configuration())
    try {
      def metainfo = AgentUtils.getApplicationMetainfo(sfs, windowsFile)
    } catch (FileNotFoundException fnfe) {
      // expected
    }
    
  }
}
