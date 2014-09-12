/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.chaos.remote

import com.jcraft.jsch.Logger
import org.apache.commons.logging.Log;

/**
 * relay from jsch to commons logging.
 * JSCH is very chatty, so it is downgraded to debug on its info ops
 */


class JschToCommonsLog implements Logger {


  final Log log

  JschToCommonsLog(Log log) {
    this.log = log
  }

  @Override
  void log(int level, String message) {
    switch (level) {
      case FATAL:
        log.fatal(message);
        break;
      case ERROR:
        log.error(message);
        break;
      case WARN:
        log.info(message);
        break;
      case INFO:
      case DEBUG:
        if (message.contains("Caught an exception")) {
          log.debug(message, new Exception('here'))
        }
        log.debug(message);
        break;
      default:
        log.info(message);
        break;
    }
  }

  @Override
  boolean isEnabled(int level) {
    switch (level) {
      case FATAL:
        return log.isFatalEnabled()
      case ERROR:
        return log.isErrorEnabled()
      case WARN:
        return log.isInfoEnabled()
      case INFO:
      case DEBUG:
        return log.isDebugEnabled()
    }
    return false
  }
}
