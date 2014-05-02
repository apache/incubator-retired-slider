#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import logging
import logging.handlers
import signal
from optparse import OptionParser
import sys
import traceback
import os
import time
import errno
import ConfigParser
import ProcessHelper
from Controller import Controller
from AgentConfig import AgentConfig
from NetUtil import NetUtil

logger = logging.getLogger()
formatstr = "%(levelname)s %(asctime)s %(filename)s:%(lineno)d - %(message)s"
agentPid = os.getpid()

configFileRelPath = "infra/conf/agent.ini"
logFileName = "agent.log"

SERVER_STATUS_URL="http://{0}:{1}{2}"


def signal_handler(signum, frame):
  #we want the handler to run only for the agent process and not
  #for the children (e.g. namenode, etc.)
  if os.getpid() != agentPid:
    os._exit(0)
  logger.info('signal received, exiting.')
  ProcessHelper.stopAgent()


def debug(sig, frame):
  """Interrupt running process, and provide a python prompt for
  interactive debugging."""
  d = {'_frame': frame}         # Allow access to frame object.
  d.update(frame.f_globals)  # Unless shadowed by global
  d.update(frame.f_locals)

  message = "Signal received : entering python shell.\nTraceback:\n"
  message += ''.join(traceback.format_stack(frame))
  logger.info(message)


def setup_logging(verbose, logfile):
  formatter = logging.Formatter(formatstr)
  rotateLog = logging.handlers.RotatingFileHandler(logfile, "a", 10000000, 25)
  rotateLog.setFormatter(formatter)
  logger.addHandler(rotateLog)

  if verbose:
    logging.basicConfig(format=formatstr, level=logging.DEBUG, filename=logfile)
    logger.setLevel(logging.DEBUG)
    logger.info("loglevel=logging.DEBUG")
  else:
    logging.basicConfig(format=formatstr, level=logging.INFO, filename=logfile)
    logger.setLevel(logging.INFO)
    logger.info("loglevel=logging.INFO")


def update_log_level(config, logfile):
  # Setting loglevel based on config file
  try:
    loglevel = config.get('agent', 'log_level')
    if loglevel is not None:
      if loglevel == 'DEBUG':
        logging.basicConfig(format=formatstr, level=logging.DEBUG, filename=logfile)
        logger.setLevel(logging.DEBUG)
        logger.info("Newloglevel=logging.DEBUG")
      else:
        logging.basicConfig(format=formatstr, level=logging.INFO, filename=logfile)
        logger.setLevel(logging.INFO)
        logger.debug("Newloglevel=logging.INFO")
  except Exception, err:
    logger.info("Default loglevel=DEBUG")


def bind_signal_handlers():
  signal.signal(signal.SIGINT, signal_handler)
  signal.signal(signal.SIGTERM, signal_handler)
  signal.signal(signal.SIGUSR1, debug)


def update_config_from_file(agentConfig):
  try:
    configFile = os.path.join(agentConfig.getWorkRootPath(), configFileRelPath)
    if os.path.exists(configFile):
      agentConfig.setConfig(configFile)
    else:
      logger.warn("No config found, using default")

  except Exception, err:
    logger.warn(err)


def perform_prestart_checks(config):
  if os.path.isfile(ProcessHelper.pidfile):
    print("%s already exists, deleting" % ProcessHelper.pidfile)
    os.remove(ProcessHelper.pidfile)
  # check if the key folders exist
  elif not os.path.isdir(config.getResolvedPath(AgentConfig.APP_PACKAGE_DIR)):
    msg = "Package dir %s does not exists, can't continue" \
          % config.getResolvedPath(AgentConfig.APP_PACKAGE_DIR)
    logger.error(msg)
    print(msg)
    sys.exit(1)

def ensure_folder_layout(config):
  ensure_path_exists(config.getResolvedPath(AgentConfig.APP_INSTALL_DIR))
  ensure_path_exists(config.getResolvedPath(AgentConfig.APP_LOG_DIR))
  ensure_path_exists(config.getResolvedPath(AgentConfig.APP_RUN_DIR))
  ensure_path_exists(config.getResolvedPath(AgentConfig.APP_TASK_DIR))
  ensure_path_exists(config.getResolvedPath(AgentConfig.LOG_DIR))
  ensure_path_exists(config.getResolvedPath(AgentConfig.RUN_DIR))

def ensure_path_exists(path):
  try:
    os.makedirs(path)
  except OSError as exception:
    if exception.errno != errno.EEXIST:
      raise
    pass
  pass

def write_pid():
  # agent only dumps self pid to file
  pid = str(os.getpid())
  file(ProcessHelper.pidfile, 'w').write(pid)


def stop_agent():
# stop existing Slider agent
  pid = -1
  try:
    f = open(ProcessHelper.pidfile, 'r')
    pid = f.read()
    pid = int(pid)
    f.close()
    os.kill(pid, signal.SIGTERM)
    time.sleep(5)
    if os.path.exists(ProcessHelper.pidfile):
      raise Exception("PID file still exists.")
  except Exception, err:
    if pid == -1:
      print ("Agent process is not running")
    else:
      os.kill(pid, signal.SIGKILL)
    os._exit(1)


def main():
  parser = OptionParser()
  parser.add_option("-v", "--verbose", dest="verbose", help="verbose log output", default=False)
  parser.add_option("-l", "--label", dest="label", help="label of the agent", default=None)
  parser.add_option("--host", dest="host", help="AppMaster host", default=None)
  parser.add_option("--port", dest="port", help="AppMaster port", default=None)
  (options, args) = parser.parse_args()

  if not 'AGENT_WORK_ROOT' in os.environ:
    parser.error("AGENT_WORK_ROOT environment variable must be set.");
  options.root_folder = os.environ['AGENT_WORK_ROOT']
  if not 'AGENT_LOG_ROOT' in os.environ:
    parser.error("AGENT_LOG_ROOT environment variable must be set.");
  options.log_folder = os.environ['AGENT_LOG_ROOT']
  if not options.label:
    parser.error("label is required.");

  bind_signal_handlers()

  # Check for configuration file.
  agentConfig = AgentConfig(options.root_folder, options.log_folder, options.label)
  update_config_from_file(agentConfig)

  # update configurations if needed
  if options.host:
      agentConfig.set(AgentConfig.SERVER_SECTION, "hostname", options.host)

  if options.port:
      agentConfig.set(AgentConfig.SERVER_SECTION, "port", options.port)

  logFile = os.path.join(agentConfig.getResolvedPath(AgentConfig.LOG_DIR), logFileName)
  perform_prestart_checks(agentConfig)
  ensure_folder_layout(agentConfig)

  setup_logging(options.verbose, logFile)
  update_log_level(agentConfig, logFile)
  write_pid()

  logger.info("Using AGENT_WORK_ROOT = " + options.root_folder)
  logger.info("Using AGENT_LOG_ROOT = " + options.log_folder)

  server_url = SERVER_STATUS_URL.format(
    agentConfig.get(AgentConfig.SERVER_SECTION, 'hostname'),
    agentConfig.get(AgentConfig.SERVER_SECTION, 'port'),
    agentConfig.get(AgentConfig.SERVER_SECTION, 'check_path'))
  print("Connecting to the server at " + server_url + "...")
  logger.info('Connecting to the server at: ' + server_url)

  # Wait until server is reachable
  netutil = NetUtil()
  netutil.try_to_connect(server_url, -1, logger)

  # Launch Controller communication
  controller = Controller(agentConfig)
  controller.start()
  try:
    while controller.is_alive():
      controller.join(timeout=1.0)
  except (KeyboardInterrupt, SystemExit):
    logger.info("... agent interrupted")
    pass


if __name__ == "__main__":
  main()
