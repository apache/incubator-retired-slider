# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup

setup(
    name = "slider-agent",
    version = "0.51.0-incubating-SNAPSHOT",
    packages = ['agent'],
    # metadata for upload to PyPI
    author = "Apache Software Foundation",
    author_email = "dev@slider.incubator.apache.org",
    description = "Slider agent",
    license = "Apache License v2.0",
    keywords = "hadoop, slider",
    url = "http://slider.incubator.apache.org/",
    long_description = "This package implements Slider for deploying and managing Apps on Yarn.",
    platforms=["any"],
    entry_points = {
        "console_scripts": [
            "slider-agent = agent.main:main",
        ],
    }
)
