
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.providers.hbase.minicluster.archives

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@CompileStatic
@Slf4j
class TestLiveClusterFromArchiveOnHDFS extends TestLiveClusterFromArchive {

  @Override
  String getTestClusterName() {
    "test_live_cluster_from_archiveonhdfs"
  }

  @Override
  boolean startHDFS() {
    true
  }

  @Override
  void setupImageToDeploy() {
    enableTestRunAgainstUploadedArchive();
  }
}
