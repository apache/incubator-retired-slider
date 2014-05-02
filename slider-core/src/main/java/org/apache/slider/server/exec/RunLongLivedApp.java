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

package org.apache.slider.server.exec;

import org.apache.hadoop.io.IOUtils;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Execute an application.
 *
 * Hadoop's Shell class isn't used because it assumes it is executing
 * a short lived application: 
 */
public class RunLongLivedApp implements Runnable {
  public static final int STREAM_READER_SLEEP_TIME = 200;
  public static final int RECENT_LINE_LOG_LIMIT = 64;
  /**
   * Class log
   */
  static final Logger LOG = LoggerFactory.getLogger(RunLongLivedApp.class);
  /**
   * Log supplied in the constructor for the spawned process
   */
  final Logger processLog;
  private final ProcessBuilder builder;
  private Process process;
  private Exception exception;
  private Integer exitCode = null;
  volatile boolean done;
  private Thread execThread;
  private Thread logThread;
  private ProcessStreamReader processStreamReader;
  //list of recent lines, recorded for extraction into reports
  private final List<String> recentLines = new LinkedList<String>();
  private final int recentLineLimit = RECENT_LINE_LOG_LIMIT;

  private ApplicationEventHandler applicationEventHandler;

  public RunLongLivedApp(Logger processLog, String... commands) {
    this.processLog = processLog;
    builder = new ProcessBuilder(commands);
    initBuilder();
  }

  public RunLongLivedApp(Logger processLog, List<String> commands) {
    this.processLog = processLog;
    builder = new ProcessBuilder(commands);
    initBuilder();
  }

  private void initBuilder() {
    builder.redirectErrorStream(false);
  }

  public ProcessBuilder getBuilder() {
    return builder;
  }

  /**
   * Set an optional application exit callback
   * @param applicationEventHandler callback to notify on application exit
   */
  public void setApplicationEventHandler(ApplicationEventHandler applicationEventHandler) {
    this.applicationEventHandler = applicationEventHandler;
  }

  /**
   * Add an entry to the environment
   * @param key key -must not be null
   * @param val value 
   */
  public void putEnv(String key, String val) {
    if (val == null) {
      throw new RuntimeException("Null value for key " + key);
    }
    builder.environment().put(key, val);
  }

  /**
   * Bulk set the environment from a map. This does
   * not replace the existing environment, just extend it/overwrite single
   * entries.
   * @param map map to add
   */
  public void putEnvMap(Map<String, String> map) {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String val = entry.getValue();
      String key = entry.getKey();
      putEnv(key, val);
    }
  }

  /**
   * Get the process environment
   * @param key
   * @return
   */
  public String getEnv(String key) {
    return builder.environment().get(key);
  }

  /**
   * Get the process reference
   * @return the process -null if the process is  not started
   */
  public Process getProcess() {
    return process;
  }

  /**
   * Get any exception raised by the process
   * @return an exception or null
   */
  public Exception getException() {
    return exception;
  }

  public List<String> getCommands() {
    return builder.command();
  }

  public String getCommand() {
    return getCommands().get(0);
  }

  /**
   * probe to see if the process is running
   * @return true iff the process has been started and is not yet finished
   */
  public boolean isRunning() {
    return process != null && !done;
  }

  /**
   * Get the exit code: null until the process has finished
   * @return the exit code or null
   */
  public Integer getExitCode() {
    return exitCode;
  }

  /**
   * Stop the process if it is running.
   * This will trigger an application completion event with the given exit code
   */
  public void stop() {
    if (!isRunning()) {
      return;
    }
    process.destroy();
  }

  /**
   * Get a text description of the builder suitable for log output
   * @return a multiline string 
   */
  protected String describeBuilder() {
    StringBuilder buffer = new StringBuilder();
    for (String arg : builder.command()) {
      buffer.append('"').append(arg).append("\" ");
    }
    return buffer.toString();
  }

  private void dumpEnv(StringBuilder buffer) {
    buffer.append("\nEnvironment\n-----------");
    Map<String, String> env = builder.environment();
    Set<String> keys = env.keySet();
    List<String> sortedKeys = new ArrayList<String>(keys);
    Collections.sort(sortedKeys);
    for (String key : sortedKeys) {
      buffer.append(key).append("=").append(env.get(key)).append('\n');
    }
  }

  /**
   * Exec the process
   * @return the process
   * @throws IOException
   */
  private Process spawnChildProcess() throws IOException, SliderException {
    if (process != null) {
      throw new SliderInternalStateException("Process already started");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Spawning process:\n " + describeBuilder());
    }
    process = builder.start();
    return process;
  }

  /**
   * Entry point for waiting for the program to finish
   */
  @Override // Runnable
  public void run() {
    LOG.debug("Application callback thread running");
    //notify the callback that the process has started
    if (applicationEventHandler != null) {
      applicationEventHandler.onApplicationStarted(this);
    }
    try {
      exitCode = process.waitFor();
    } catch (InterruptedException e) {
      LOG.debug("Process wait interrupted -exiting thread");
    } finally {
      //here the process has finished
      LOG.info("process has finished");
      //tell the logger it has to finish too
      done = true;

      //now call the callback if it is set
      if (applicationEventHandler != null) {
        applicationEventHandler.onApplicationExited(this, exitCode);
      }
      try {
        logThread.join();
      } catch (InterruptedException ignored) {
        //ignored
      }
    }
  }

  /**
   * Create a thread to wait for this command to complete.
   * THE THREAD IS NOT STARTED.
   * @return the thread
   * @throws IOException Execution problems
   */
  private Thread spawnIntoThread() throws IOException, SliderException {
    spawnChildProcess();
    return new Thread(this, getCommand());
  }

  /**
   * Spawn the application
   * @throws IOException IO problems
   * @throws SliderException internal state of this class is wrong
   */
  public void spawnApplication() throws IOException, SliderException {
    execThread = spawnIntoThread();
    execThread.start();
    processStreamReader =
      new ProcessStreamReader(processLog, STREAM_READER_SLEEP_TIME);
    logThread = new Thread(processStreamReader, "IO");
    logThread.start();
  }

  /**
   * Get the lines of recent output
   * @return the last few lines of output; an empty list if there are none
   * or the process is not actually running
   */
  public synchronized List<String> getRecentOutput() {
    return new ArrayList<String>(recentLines);
  }


  /**
   * add the recent line to the list of recent lines; deleting
   * an earlier on if the limit is reached.
   *
   * Implementation note: yes, a circular array would be more
   * efficient, especially with some power of two as the modulo,
   * but is it worth the complexity and risk of errors for
   * something that is only called once per line of IO?
   * @param line line to record
   * @param isErrorStream is the line from the error stream
   */
  private synchronized void recordRecentLine(String line,
                                             boolean isErrorStream) {
    if (line == null) {
      return;
    }
    String entry = (isErrorStream ? "[ERR] " : "[OUT] ") + line;
    recentLines.add(entry);
    if (recentLines.size() > recentLineLimit) {
      recentLines.remove(0);
    }
  }

  /**
   * Class to read data from the two process streams, and, when run in a thread
   * to keep running until the <code>done</code> flag is set. 
   * Lines are fetched from stdout and stderr and logged at info and error
   * respectively.
   */

  private class ProcessStreamReader implements Runnable {
    private final Logger streamLog;
    private final int sleepTime;

    private ProcessStreamReader(Logger streamLog, int sleepTime) {
      this.streamLog = streamLog;
      this.sleepTime = sleepTime;
    }

    private int readCharNonBlocking(BufferedReader reader) throws IOException {
      if (reader.ready()) {
        return reader.read();
      } else {
        return -1;
      }
    }

    /**
     * Read in a line, or, if the limit has been reached, the buffer
     * so far
     * @param reader source of data
     * @param line line to build
     * @param limit limit of line length
     * @return true if the line can be printed
     * @throws IOException IO trouble
     */
    private boolean readAnyLine(BufferedReader reader,
                                StringBuilder line,
                                int limit)
      throws IOException {
      int next;
      while ((-1 != (next = readCharNonBlocking(reader)))) {
        if (next != '\n') {
          line.append((char) next);
          limit--;
          if (line.length() > limit) {
            //enough has been read in to print it any
            return true;
          }
        } else {
          //line end return flag to say so
          return true;
        }
      }
      //here the end of the stream is hit, or the limit
      return false;
    }


    @Override //Runnable
    @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
    public void run() {
      BufferedReader errReader = null;
      BufferedReader outReader = null;
      StringBuilder outLine = new StringBuilder(256);
      StringBuilder errorLine = new StringBuilder(256);
      try {
        errReader = new BufferedReader(new InputStreamReader(process
                                                               .getErrorStream()));
        outReader = new BufferedReader(new InputStreamReader(process
                                                               .getInputStream()));
        while (!done) {
          boolean processed = false;
          if (readAnyLine(errReader, errorLine, 256)) {
            String line = errorLine.toString();
            recordRecentLine(line, true);
            streamLog.warn(line);
            errorLine.setLength(0);
            processed = true;
          }
          if (readAnyLine(outReader, outLine, 256)) {
            String line = outLine.toString();
            recordRecentLine(line, false);
            streamLog.info(line);
            outLine.setLength(0);
            processed |= true;
          }
          if (!processed) {
            //nothing processed: wait a bit for data.
            try {
              Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
              //ignore this, rely on the done flag
              LOG.debug("Ignoring ", e);
            }
          }
        }
        //get here, done time

        //print the current error line then stream through the rest
        streamLog.error(errorLine.toString());
        String line = errReader.readLine();
        while (line != null) {
          streamLog.error(line);
          if (Thread.interrupted()) {
            break;
          }
          line = errReader.readLine();
          recordRecentLine(line, true);
        }
        //now do the info line
        streamLog.info(outLine.toString());
        line = outReader.readLine();
        while (line != null) {
          streamLog.info(line);
          if (Thread.interrupted()) {
            break;
          }
          line = outReader.readLine();
          recordRecentLine(line, false);
        }

      } catch (Exception ignored) {
        LOG.warn("encountered ", ignored);
        //process connection has been torn down
      } finally {
        IOUtils.closeStream(errReader);
        IOUtils.closeStream(outReader);
      }
    }
  }
}
