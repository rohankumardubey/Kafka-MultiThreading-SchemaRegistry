package com.modeln.intg.client.SamplePocCode.UsingSchemaRegistry.RegisterFromSchemaRegistry;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SchemaMain {

    final static String Topic="transcations";
    public static String  SchemaReturn() throws IOException {

        String result=null;

        String url="http://localhost:8081/subjects/"+Topic+"-value/versions/latest/schema";
        String[] command = {"curl","--silent","-X","GET", url};
        ProcessBuilder process = new ProcessBuilder(command);
        Process p;
        try
        {
            p = process.start();
            BufferedReader reader =  new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line = null;
            while ( (line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            result= builder.toString();


        }
        catch (IOException e)
        {   System.out.print("error");
            e.printStackTrace();
        }

        return result;

    }
}
