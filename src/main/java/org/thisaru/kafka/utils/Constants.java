/*
 * Author: Thisaru Guruge
 * Website: https://ThisaruGuruge.github.io
 *
 * This software is under Apache 2.0 License.
 *
 * Copyright (c) 2020 | All Rights Reserved
 */

package org.thisaru.kafka.utils;

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
