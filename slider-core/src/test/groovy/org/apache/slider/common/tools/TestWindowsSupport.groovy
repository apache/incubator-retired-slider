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
import org.apache.hadoop.service.ServiceStateException
import org.apache.hadoop.util.Shell
import org.apache.slider.providers.agent.AgentUtils
import org.apache.slider.test.YarnMiniClusterTestBase
import org.junit.Test

import java.util.regex.Pattern

@CompileStatic
@Slf4j
class TestWindowsSupport extends YarnMiniClusterTestBase {

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
    assumeWindows()

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
    assumeWindows()
    SliderFileSystem sfs = new SliderFileSystem(new Configuration())
    try {
      def metainfo = AgentUtils.getApplicationMetainfo(sfs, windowsFile, false)
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }


/*
  @Test
  public void testHasGawkInstalled() throws Throwable {
    assume(Shell.WINDOWS, "not windows")
    exec(0, ["gawk", "--version"])
  }
*/

/*
  @Test
  public void testHasXargsInstalled() throws Throwable {
    assume(Shell.WINDOWS, "not windows")
    exec(0, ["xargs", "--version"])
  }
*/

  @Test
  public void testExecNonexistentBinary() throws Throwable {
    assumeWindows()
    def commands = ["undefined-application", "--version"]
    try {
      exec(0, commands)
      fail("expected an exception")
    } catch (ServiceStateException e) {
      if (!(e.cause instanceof FileNotFoundException)) {
        throw e;
      }
    }
  }
  @Test
  public void testExecNonexistentBinary2() throws Throwable {
    assumeWindows()
    assert !doesAppExist(["undefined-application", "--version"])
  }

  @Test
  public void testEmitKillCommand() throws Throwable {

    def result = killJavaProcesses("regionserver", 9)
    // we know the exit code if there is no supported kill operation
    assert kill_supported || result == -1
  }

  @Test
  public void testHadoopHomeDefined() throws Throwable {
    assumeWindows()
    def hadoopHome = Shell.hadoopHome
    log.info("HADOOP_HOME=$hadoopHome")
  }
  
  @Test
  public void testHasWinutils() throws Throwable {
    assumeWindows()
    SliderUtils.maybeVerifyWinUtilsValid()
  }

  @Test
  public void testExecWinutils() throws Throwable {
    assumeWindows()
    def winUtilsPath = Shell.winUtilsPath
    assert winUtilsPath
    File winUtils = new File(winUtilsPath)
    log.debug("Winutils is at $winUtils)")

    exec(0, [winUtilsPath, "systeminfo"])
  }

  @Test
  public void testPath() throws Throwable {
    String path = extractPath()
    log.info("Path value = $path")
  }

  @Test
  public void testFindJavac() throws Throwable {
    String name = Shell.WINDOWS ? "javac.exe" : "javac"
    assert locateExecutable(name)
  }
  
  @Test
  public void testHadoopDLL() throws Throwable {
    assumeWindows()
    // split the path
    File exepath = locateExecutable("HADOOP.DLL")
    assert exepath
    log.info "Hadoop DLL at: $exepath"
  }

}
