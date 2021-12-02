package com.modeln.intg.client.SamplePocCode.UsingSchemaRegistry.HttpsResponseFetch;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class test {
    public static void main(String[] args) throws Exception {
//        URL url = new URL("http://example.net/new-message.php");
//        Map<String,Object> params = new LinkedHashMap<>();
//        params.put("name", "Freddie the Fish");
//        params.put("email", "fishie@seamail.example.com");
//        params.put("reply_to_thread", 10394);
//        params.put("message", "Shark attacks in Botany Bay have gotten out of control. We need more defensive dolphins to protect the schools here, but Mayor Porpoise is too busy stuffing his snout with lobsters. He's so shellfish.");
//
//        StringBuilder postData = new StringBuilder();
//        for (Map.Entry<String,Object> param : params.entrySet()) {
//            if (postData.length() != 0) postData.append('&');
//            postData.append(URLEncoder.encode(param.getKey(), StandardCharsets.UTF_8));
//            postData.append('=');
//            postData.append(URLEncoder.encode(String.valueOf(param.getValue()), "UTF-8"));
//        }

//        byte[] postDataBytes = postData.toString().getBytes(StandardCharsets.UTF_8);
//        System.out.println(postData);
//
//        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
//        conn.setRequestMethod("POST");
//        conn.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
//        conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length));
//        conn.setDoOutput(true);
//        conn.getOutputStream().write(postDataBytes);
//
//        Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
//
//        for (int c; (c = in.read()) >= 0;)
//            System.out.print((char)c);


        //---------------------------------------------------


//        String urlParameters = "--data '{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}' ";
//        URL url = new URL("http://localhost:8081/subjects/sample-key/versions/latest/schema");
//        URLConnection conn = url.openConnection();
//
//        conn.setDoOutput(true);
//
//        OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
//
//        writer.write(urlParameters);
//        writer.flush();
//
//        String line;
//        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//
//        while ((line = reader.readLine()) != null) {
//            System.out.println(line);
//        }
//        writer.close();
//        reader.close();

        //-------------------------------
//        URL url = new URL("http://localhost:8081/subjects/sample-key/versions/latest/schema");
//        URLConnection con = url.openConnection();
//        HttpURLConnection http = (HttpURLConnection)con;
//        http.setRequestMethod("POST"); // PUT is another valid option
//        http.setDoOutput(true);
//        byte[] out = "{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}" .getBytes(StandardCharsets.UTF_8);
//        int length = out.length;
//
//        http.setFixedLengthStreamingMode(length);
//        http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
//        http.connect();
//        try(OutputStream os = http.getOutputStream()) {
//            os.write(out);
//        }
// Do something with http.getInputStream()

    //----------------------------------------
//        CloseableHttpClient httpclient = HttpClients.createDefault();
//        HttpPost httpPost = new HttpPost("http://localhost:8081/subjects/sample-key/versions");
//        String JSON_STRING="{\"compatibility\": \"FULL\"}";
//        HttpEntity stringEntity = new StringEntity(JSON_STRING, ContentType.APPLICATION_JSON);
//        httpPost.setEntity(stringEntity);
//        CloseableHttpResponse response2 = httpclient.execute(httpPost);
//        System.out.println(response2);

        URL url = new URL ("http://localhost:8081/subjects/sample-key/versions/latest/schema");
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);

        String jsonInputString = "'{\"schema\": \"{\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"my_field1\",\"type\": \"int\"},{\"name\": \"my_field2\",\"type\": \"int\"},{\"name\": \"my_field3\",\"type\": \"int\"}],\"name\": \"sampleRecord\",\"namespace\": \"com.mycorp.mynamespace\",\"type\":\"record\"}\"}'";
        try(OutputStream os = con.getOutputStream()) {
            byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            System.out.println(response.toString());
        }
    }
}

