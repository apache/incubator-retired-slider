"""
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

Slider Agent

"""

from resource_management.core.providers import Provider
from resource_management.core.logger import Logger
from resource_management.core.base import Fail
from resource_management.core import ExecuteTimeoutException
from multiprocessing import Queue
import time
import os
import subprocess
import shutil
from resource_management.libraries.script import Script


def _merge_env(env1, env2, merge_keys=['PYTHONPATH']):
  """
  Merge env2 into env1. Also current python instance variables from merge_keys list taken into account and they will be
  merged with equivalent keys from env1 and env2 using system path separator.
  :param env1: first environment, usually returned by CreateEnvironmentBlock
  :param env2: custom environment
  :param merge_keys: env variables to merge as PATH
  :return: merged environment
  """
  env1 = dict(env1)  # copy to new dict in case env1 is os.environ
  if env2:
    for key, value in env2.iteritems():
      if not key in merge_keys:
        env1[key] = value
  # strnsform keys and values to str(windows can not accept unicode)
  result_env = {}
  for key, value in env1.iteritems():
    if not key in merge_keys:
      result_env[str(key)] = str(value)
  #merge keys from merge_keys
  def put_values(key, env, result):
    if env and key in env:
      result.extend(env[key].split(os.pathsep))

  for key in merge_keys:
    all_values = []
    for env in [env1, env2, os.environ]:
      put_values(key, env, all_values)
    result_env[str(key)] = str(os.pathsep.join(set(all_values)))
  return result_env


# Execute command. As windows stack heavily relies on proper environment it is better to reload fresh environment
# on every execution. env variable will me merged with fresh environment for user.
def _call_command(command, logoutput=False, cwd=None, env=None, wait_for_finish=True, timeout=None, user=None,
                  pid_file_name=None, poll_after=None):
  # TODO implement user
  Logger.info("Executing %s" % (command))
  proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          cwd=cwd, env=env, shell=False)
  code = None
  logAnyway = False
  if not wait_for_finish:
    Logger.debug("No need to wait for the process to exit. Will leave the process running ...")
    code = 0
    logAnyway = False
    if pid_file_name:
      Logger.debug("Writing the process id %s to file %s" % (str(proc.pid), pid_file_name))
      pidfile = open(pid_file_name, 'w')
      pidfile.write(str(proc.pid))
      pidfile.close()
      Logger.info("Wrote the process id to file %s" % pid_file_name)

      ## wait poll_after seconds and poll
    if poll_after:
      time.sleep(poll_after)
      if proc.poll() is None:
        return code, None, None  # if still running then return
      else:
        logAnyway = True  # assume failure and log
        Logger.warning("Process is not up after the polling interval " + str(poll_after) + " seconds.")
    else:
      return code, None, None

  if timeout:
    q = Queue()
    t = threading.Timer(timeout, on_timeout, [proc, q])
    t.start()

  out, err = proc.communicate()
  code = proc.returncode

  if logoutput or logAnyway:
    if out:
      Logger.info("Out: " + str(out))
    if err:
      Logger.info("Err: " + str(err))
    if code:
      Logger.info("Ret Code: " + str(code))

  return code, out, err

# see msdn Icacls doc for rights
def _set_file_acl(file, user, rights):
  acls_modify_cmd = "icacls {0} /grant {1}:{2}".format(file, user, rights)
  acls_remove_cmd = "icacls {0} /remove {1}".format(file, user)
  code, out, err = _call_command(acls_remove_cmd)
  if code != 0:
    raise Fail("Can not remove rights for path {0} and user {1}".format(file, user))
  code, out, err = _call_command(acls_modify_cmd)
  if code != 0:
    raise Fail("Can not set rights {0} for path {1} and user {2}".format(rights, file, user))
  else:
    return

class FileProvider(Provider):
  def action_create(self):
    path = self.resource.path

    if os.path.isdir(path):
      raise Fail("Applying %s failed, directory with name %s exists" % (self.resource, path))

    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
      raise Fail("Applying %s failed, parent directory %s doesn't exist" % (self.resource, dirname))

    write = False
    content = self._get_content()
    if not os.path.exists(path):
      write = True
      reason = "it doesn't exist"
    elif self.resource.replace:
      if content is not None:
        with open(path, "rb") as fp:
          old_content = fp.read()
        if content != old_content:
          write = True
          reason = "contents don't match"
          if self.resource.backup:
            self.resource.env.backup_file(path)

    if write:
      Logger.info("Writing %s because %s" % (self.resource, reason))
      with open(path, "wb") as fp:
        if content:
          fp.write(content)

    if self.resource.owner and self.resource.mode:
      _set_file_acl(self.resource.path, self.resource.owner, self.resource.mode)

  def action_delete(self):
    path = self.resource.path

    if os.path.isdir(path):
      raise Fail("Applying %s failed, %s is directory not file!" % (self.resource, path))

    if os.path.exists(path):
      Logger.info("Deleting %s" % self.resource)
      os.unlink(path)

  def _get_content(self):
    content = self.resource.content
    if content is None:
      return None
    elif isinstance(content, basestring):
      return content
    elif hasattr(content, "__call__"):
      return content()
    raise Fail("Unknown source type for %s: %r" % (self, content))

class ExecuteProvider(Provider):
  def action_run(self):
    if self.resource.creates:
      if os.path.exists(self.resource.creates):
        return

    Logger.debug("Executing %s" % self.resource)

    if self.resource.path != []:
      if not self.resource.environment:
        self.resource.environment = {}

      self.resource.environment['PATH'] = os.pathsep.join(self.resource.path)

    for i in range(0, self.resource.tries):
      try:
        code, _, _  = _call_command(self.resource.command, logoutput=self.resource.logoutput,
                                    cwd=self.resource.cwd, env=self.resource.environment,
                                    wait_for_finish=self.resource.wait_for_finish,
                                    timeout=self.resource.timeout, user=self.resource.user,
                                    pid_file_name=self.resource.pid_file, poll_after=self.resource.poll_after)
        if code != 0 and not self.resource.ignore_failures:
          raise Fail("Failed to execute " + self.resource.command)
        break
      except Fail as ex:
        Logger.info("Error raised: %s" % str(ex))
        if i == self.resource.tries - 1:  # last try
          raise ex
        else:
          Logger.info("Retrying after %d seconds. Reason: %s" % (self.resource.try_sleep, str(ex)))
          time.sleep(self.resource.try_sleep)
      except ExecuteTimeoutException:
        err_msg = ("Execution of '%s' was killed due timeout after %d seconds") % (
          self.resource.command, self.resource.timeout)

        if self.resource.on_timeout:
          Logger.info("Executing '%s'. Reason: %s" % (self.resource.on_timeout, err_msg))
          _call_command(self.resource.on_timeout)
        else:
          raise Fail(err_msg)


class DirectoryProvider(Provider):
  def action_create(self):
    path = DirectoryProvider._trim_uri(self.resource.path)
    if not os.path.exists(path):
      Logger.info("Creating directory %s" % self.resource)
      if self.resource.recursive:
        os.makedirs(path)
      else:
        dirname = os.path.dirname(path)
        if not os.path.isdir(dirname):
          raise Fail("Applying %s failed, parent directory %s doesn't exist" % (self.resource, dirname))

        os.mkdir(path)

    if not os.path.isdir(path):
      raise Fail("Applying %s failed, file %s already exists" % (self.resource, path))

    if self.resource.owner and self.resource.mode:
      _set_file_acl(path, self.resource.owner, self.resource.mode)

  def action_delete(self):
    path = self.resource.path
    if os.path.exists(path):
      if not os.path.isdir(path):
        raise Fail("Applying %s failed, %s is not a directory" % (self.resource, path))

      Logger.info("Removing directory %s and all its content" % self.resource)
      shutil.rmtree(path)

  @staticmethod
  def _trim_uri(file_uri):
    if file_uri.startswith("file:///"):
      return file_uri[8:]
    return file_uri
