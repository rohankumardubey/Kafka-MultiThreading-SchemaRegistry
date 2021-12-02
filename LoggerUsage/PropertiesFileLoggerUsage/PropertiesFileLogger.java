package com.modeln.intg.client.SamplePocCode.LoggerUsage.PropertiesFileLoggerUsage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PropertiesFileLogger {
    private static Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        logger.debug("Debug Message Logged !!!");
        logger.info("Info Message Logged !!!");
        logger.error("Error Message Logged !!!", new NullPointerException("NullError"));
    }
}
