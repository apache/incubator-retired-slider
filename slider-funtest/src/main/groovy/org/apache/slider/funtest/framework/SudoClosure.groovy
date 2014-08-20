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

import org.apache.hadoop.security.UserGroupInformation

import java.security.PrivilegedExceptionAction

/**
 * Bridge from groovy closures to doAs
 * @param < T >
 */
class SudoClosure<T> implements PrivilegedExceptionAction<T> {
  
  final Closure<T> closure;

  SudoClosure(Closure<T> closure) {
    this.closure = closure
  }

  @Override
  T run() throws Exception {
    return closure()
  }

  /**
   * 
   * @param user
   * @param closure
   * @return
   */
  public static <T2> T2 sudo(UserGroupInformation user,
      Closure<T2> closure) {
    
    user.doAs(new SudoClosure<T2>(closure))
    
  }
}
