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

package org.apache.slider.providers.accumulo.funtest

import org.apache.hadoop.security.UserGroupInformation
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.FileUploader
import org.apache.slider.providers.accumulo.AccumuloClientProvider
import org.junit.BeforeClass

import static SliderXMLConfKeysForTesting.KEY_TEST_ACCUMULO_APPCONF
import static SliderXMLConfKeysForTesting.KEY_TEST_ACCUMULO_TAR
import static org.apache.slider.api.ResourceKeys.YARN_MEMORY
import static org.apache.slider.common.SliderXMLConfKeysForTesting.DEFAULT_ACCUMULO_LAUNCH_TIME_SECONDS
import static org.apache.slider.common.SliderXMLConfKeysForTesting.KEY_ACCUMULO_LAUNCH_TIME
import static org.apache.slider.common.SliderXMLConfKeysForTesting.KEY_TEST_ACCUMULO_ENABLED
import static org.apache.slider.providers.accumulo.AccumuloKeys.*
import static org.apache.slider.common.params.Arguments.ARG_PROVIDER
import static org.apache.slider.common.params.Arguments.ARG_RES_COMP_OPT

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.fate.ZooStore
import org.apache.accumulo.trace.instrument.Tracer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.filecache.DistributedCache
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.SliderXMLConfKeysForTesting
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.providers.accumulo.AccumuloKeys
import org.apache.slider.common.params.Arguments
import org.apache.thrift.TException
import org.junit.Before

/**
 * Anything specific to accumulo tests
 */
abstract class AccumuloCommandTestBase extends CommandTestBase
  implements SliderExitCodes, SliderActions {

  public static final int ACCUMULO_LAUNCH_WAIT_TIME
  public static final boolean ACCUMULO_TESTS_ENABLED
  public static final FileUploader uploader
  public Path ACCUMULO_TAR
  public Path ACCUMULO_CONF

  static {
    ACCUMULO_LAUNCH_WAIT_TIME = getTimeOptionMillis(SLIDER_CONFIG,
        KEY_ACCUMULO_LAUNCH_TIME,
        1000 * DEFAULT_ACCUMULO_LAUNCH_TIME_SECONDS)
    ACCUMULO_TESTS_ENABLED =
        SLIDER_CONFIG.getBoolean(KEY_TEST_ACCUMULO_ENABLED, false)
    uploader = new FileUploader(SLIDER_CONFIG, UserGroupInformation.currentUser)
  }


  public static void assumeAccumuloTestsEnabled() {
    assume(ACCUMULO_TESTS_ENABLED, "Accumulo tests disabled")
  }
  
  @BeforeClass
  public static void verifyPreconditions() {
    //if tests are not enabled: skip tests
    assumeAccumuloTestsEnabled()
    // but if they are -fail if the values are missing
    getRequiredConfOption(SLIDER_CONFIG, OPTION_ZK_HOME)
    getRequiredConfOption(SLIDER_CONFIG, OPTION_HADOOP_HOME)
  }

  @BeforeClass
  public static void extendClasspath() {
    addExtraJar(AccumuloClientProvider)
  }

  @Before
  public void uploadFiles() {
    File tar = new File(getRequiredConfOption(SLIDER_CONFIG,
      KEY_TEST_ACCUMULO_TAR))
    File conf = new File(getRequiredConfOption(SLIDER_CONFIG,
      KEY_TEST_ACCUMULO_APPCONF))

    //create the home dir or fail
    Path home = uploader.mkHomeDir()

    ACCUMULO_TAR = new Path(home, tar.getName())
    ACCUMULO_CONF = new Path(home, "accumulo-conf")

    // Upload the local accumulo tarball and conf directory to hdfs
    uploader.copyIfOutOfDate(tar, ACCUMULO_TAR, false)
    uploader.copyIfOutOfDate(conf, ACCUMULO_CONF, false)
  }

  /**
   * Create an accumulo cluster
   *
   * @param clustername
   * @param roles
   * @param argsList
   * @param blockUntilRunning
   * @param containerMemory
   * @return
   */
  public SliderShell createAccumuloCluster(String clustername,
                                         Map<String, Integer> roles,
                                         List<String> argsList,
                                         boolean blockUntilRunning,
                                         Map<String, String> clusterOps,
                                         String containerMemory,
                                         String password) {
    argsList << ARG_PROVIDER << PROVIDER_ACCUMULO;


    YarnConfiguration conf = SLIDER_CONFIG
    clusterOps[OPTION_ZK_HOME] = getRequiredConfOption(
        SLIDER_CONFIG, OPTION_ZK_HOME)
    clusterOps[OPTION_HADOOP_HOME] = getRequiredConfOption(
        SLIDER_CONFIG,
        OPTION_HADOOP_HOME)
    argsList << Arguments.ARG_IMAGE << ACCUMULO_TAR

    argsList << Arguments.ARG_CONFDIR << ACCUMULO_CONF

    argsList << Arguments.ARG_OPTION << AccumuloKeys.OPTION_ACCUMULO_PASSWORD << password

    argsList << ARG_RES_COMP_OPT << ROLE_MASTER <<
    YARN_MEMORY << containerMemory
    argsList << ARG_RES_COMP_OPT << ROLE_TABLET <<
    YARN_MEMORY << containerMemory
    argsList << ARG_RES_COMP_OPT << ROLE_MONITOR <<
    YARN_MEMORY << containerMemory
    argsList << ARG_RES_COMP_OPT << ROLE_GARBAGE_COLLECTOR <<
    YARN_MEMORY << containerMemory

    return createSliderApplication(clustername,
                             roles,
                             argsList,
                             blockUntilRunning,
                             clusterOps)
  }

  public boolean loadClassesForMapReduce(Configuration conf) {
    String[] neededClasses = [AccumuloInputFormat.class.getName(), TException.class.getName(), ZooStore.class.getName(), Tracer.class.getName()]
    String[] neededJars = ["accumulo-core.jar", "libthrift.jar", "accumulo-fate.jar", "accumulo-trace.jar"]
    
    LocalFileSystem localfs = new LocalFileSystem();
    localfs.initialize(new URI("file:///"), conf);
    ArrayList<Path> jarsToLoad = new ArrayList<Path>();
    
    ClassLoader loader = AccumuloCommandTestBase.class.getClassLoader();
    boolean missingJar = false
    try {
      for (String className : neededClasses) {
        className = className.replace('.', '/') + ".class"
        URL url = loader.getResource(className)
        log.debug("For $className found $url")
        String path = url.getPath();
        int separator = path.indexOf('!')
        if (-1 == separator) {
          log.info("Could not interpret $path to find a valid path to a jar")
          missingJar = true;
          break;
        }
        path = path.substring(0, separator)
        Path jarPath = new Path(path);
        if (!localfs.exists(jarPath)) {
          log.info("Could not find $jarPath")
          missingJar = true
          jarsToLoad.clear();
          break
        } else {
          jarsToLoad.add(jarPath);
        }
      }
    } catch (Exception e) {
      log.warn("Got exception trying to parse jars from maven repository", e)
      missingJar = true
    }

    if (missingJar) { 
      String accumuloHome = conf.get(SliderXMLConfKeysForTesting.KEY_TEST_ACCUMULO_HOME)
      if (null == accumuloHome) {
        log.info(SliderXMLConfKeysForTesting.KEY_TEST_ACCUMULO_HOME + " is not defined in Slider configuration. Cannot load jars from local Accumulo installation")
      } else {
        Path p = new Path(accumuloHome + "/lib")
        if (localfs.exists(p)) {
          log.info("Found lib directory in local accumulo home: $p")
          for (String neededJar : neededJars) {
            Path jarPath = new Path(p, neededJar);
            if (!localfs.exists(jarPath)) {
              log.info("Could not find " + jarPath)
              missingJar = true
              jarsToLoad.clear();
              break
            } else {
              jarsToLoad.add(jarPath);
            }
          }
        }
      }
    }
      
    if (!missingJar) {
      for (Path neededJar : jarsToLoad) {
        log.info("Adding to mapreduce classpath: $neededJar")
        DistributedCache.addArchiveToClassPath(neededJar, conf, localfs)
      }
      return true
    } else {
      log.info("Falling back to local mapreduce because the necessary Accumulo classes couldn't be loaded")
    }
    
    return false
  }
  
  public void tryToLoadMapredSite(Configuration conf) {
    String hadoopHome = conf.get(AccumuloKeys.OPTION_HADOOP_HOME)
    
    // Add mapred-site.xml if we can find it
    if (null == hadoopHome) {
      log.info(AccumuloKeys.OPTION_HADOOP_HOME + " was not defined in Slider configuration. Running job in local mode");
    } else {
      LocalFileSystem localfs = new LocalFileSystem();
      localfs.initialize(new URI("file:///"), conf);
          
      // If we found the necessary jars, make sure we throw mapred-site.xml on the classpath
      // too so that we avoid local mode
      Path p = new Path(hadoopHome + "/etc/hadoop/mapred-site.xml");
      if (localfs.exists(p)) {
        log.info("Loaded mapred-site.xml from " + p);
        conf.addResource(p);
      } else {
        log.info("Failed to load mapred-site.xml as it doesn't exist at " + p);
      }
    }
  }
}
