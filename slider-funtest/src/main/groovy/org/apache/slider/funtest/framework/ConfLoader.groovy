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
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.tools.ConfigHelper

import static org.apache.slider.funtest.framework.FuntestProperties.*

@Slf4j
public class ConfLoader {

  public static YarnConfiguration loadSliderConf(File confFile, boolean loadHadoopConfDir = true) {
    URI confURI = confFile.toURI();
    YarnConfiguration conf = new YarnConfiguration()
    def confXmlUrl = confURI.toURL()
    conf.addResource(confXmlUrl)
    conf.set(KEY_TEST_CONF_XML, confXmlUrl.toString())

    def sliderConfDir = confFile.parent
    conf.set(KEY_TEST_CONF_DIR, sliderConfDir)
    conf.set(ENV_SLIDER_CONF_DIR, sliderConfDir)
    ConfigHelper.addEnvironmentVariables(conf, ENV_PREFIX)
    
    if (loadHadoopConfDir) {
      def hadoopConf = conf.getTrimmed(ENV_HADOOP_CONF_DIR)
      if (hadoopConf) {
        File hadoopConfDir = new File(hadoopConf).canonicalFile
        def hadoopConfDirPath = hadoopConfDir.absolutePath
        if (!hadoopConfDir.directory) {
          throw new FileNotFoundException(hadoopConfDirPath)
        }
        log.debug("$ENV_HADOOP_CONF_DIR=$hadoopConfDirPath —loading")
        // a conf dir has been set. Load the values locally
        maybeAddConfFile(conf, hadoopConfDir, CORE_SITE_XML)
        maybeAddConfFile(conf, hadoopConfDir, HDFS_SITE_XML)
        maybeAddConfFile(conf, hadoopConfDir, YARN_SITE_XML)
        // now need to force in the original again because otherwise
        // the -site.xml values will overwrite the -client ones
        ConfigHelper.addConfigurationFile(conf, confFile, true)
      }

    }
    
    return conf
  }

  /**
   * Add a configuration file at a given path
   * @param dir directory containing the file
   * @param filename filename
   * @return true if the file was found
   * @throws IOException loading problems (other than a missing file)
   */
  public static boolean maybeAddConfFile(Configuration conf, File dir, String filename)
  throws IOException {
    File confFile = new File(dir, filename)
    if (confFile.file) {
      ConfigHelper.addConfigurationFile(conf, confFile, true)
      log.info("Loaded ${confFile.absolutePath}")
      return true;
    } else {
      log.info("Did not find ${confFile.absolutePath} —skipping load")
      return false;
    }
  }

}
