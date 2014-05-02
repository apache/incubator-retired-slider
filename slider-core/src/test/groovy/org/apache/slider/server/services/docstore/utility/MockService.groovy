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

package org.apache.slider.server.services.docstore.utility

import org.apache.hadoop.service.AbstractService
import org.apache.hadoop.service.ServiceStateException
import org.apache.slider.core.main.ExitCodeProvider

/**
 * Little mock service to simulate delays
 */
class MockService extends AbstractService implements ExitCodeProvider {

  boolean fail = false;
  int exitCode;
  int lifespan = -1;

  MockService() {
    super("mock")
  }

  MockService(String name, boolean fail, int lifespan) {
    super(name)
    this.fail = fail
    this.lifespan = lifespan;
  }

  @Override
  protected void serviceStart() throws Exception {
    //act on the lifespan here
    if (lifespan > 0) {
      Thread.start {
        Thread.sleep(lifespan)
        finish()
      }
    } else {
      if (lifespan == 0) {
        finish();
      } else {
        //continue until told not to
      }
    }
  }

  void finish() {
    if (fail) {
      ServiceStateException e = new ServiceStateException("$name failed")
      noteFailure(e);
      stop();
      throw e
    } else {
      stop();
    }
  }

}
