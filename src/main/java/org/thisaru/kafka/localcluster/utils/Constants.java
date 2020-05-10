/*
 *    Copyright 2020 Thisaru Guruge
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.thisaru.kafka.localcluster.utils;

import java.nio.file.Paths;

public class Constants {
    private Constants() {

    }

    private static final String PROPERTIES_DIRECTORY = "/properties";
    private static final String ZOOKEEPER_PROP_FILE = "zookeeper.properties";
    private static final String KAFKA_PROP_FILE = "server.properties";

    public static final String ZOOKEEPER_PROP = Paths.get(PROPERTIES_DIRECTORY, ZOOKEEPER_PROP_FILE).toString();
    public static final String KAFKA_PROP = Paths.get(PROPERTIES_DIRECTORY, KAFKA_PROP_FILE).toString();

    // Suffixes
    public static final String ZOOKEEPER_SUFFIX = "zookeeper";
    public static final String KAFKA_SUFFIX = "kafka";
}
