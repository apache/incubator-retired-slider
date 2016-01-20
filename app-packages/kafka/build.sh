#! /bin/bash

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


wget http://apache.websitebeheerjd.nl/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz -O $HOME/kafka_2.10-0.8.2.1.tgz
mvn clean install -DskipTests -Dkafka.src=$HOME/kafka_2.10-0.8.2.1.tgz -Dkafka.version=kafka_2.10-0.8.2.1
unzip -o target/slider-kafka-app-package-0.90.0-incubating-SNAPSHOT.zip appConfig.json
