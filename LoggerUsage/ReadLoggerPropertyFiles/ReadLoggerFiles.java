package com.modeln.intg.client.SamplePocCode.LoggerUsage.ReadLoggerPropertyFiles;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReadLoggerFiles {
        public static void main(String[] args) throws IOException {
        Properties sample = new Properties();
        InputStream in = ReadLoggerFiles.class.getClassLoader().getResourceAsStream("log4j2.properties");
        sample.load(in);
        System.out.println(sample.getProperty("appender.console.type"));
    }
}
