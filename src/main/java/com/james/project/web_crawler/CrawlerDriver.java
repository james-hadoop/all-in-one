package com.james.project.web_crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class CrawlerDriver {

    public static void main(String[] args) {
        BufferedWriter writer = null;

        try {
            List<Map<String, String>> parsedContentMapList = new ArrayList<Map<String, String>>();

            int beginPageNumber =552;
            int endPageNumber = 633;
            // int endPageNumber = 1;

            String keyword = "企业所得税";

            String path = "46.csv";

            File file = new File(path);
            file.createNewFile();

            writer = new BufferedWriter(new FileWriter(path));
            writer.write("keyword,url,value");
            writer.newLine();

            for (int i = beginPageNumber; i <= endPageNumber; i++) {
                System.out.println("page number: " + i);
                String url = generateUrl(i);

                String newsSummary = getHttpContent(url);

                List<String> newsUrls = extractSubUrl(newsSummary);

                for (String newsUrl : newsUrls) {
                    Map<String, String> parsedContentMap = extractContent(newsUrl, keyword);
                    saveMap(parsedContentMap, writer);
                    // parsedContentMapList.add(parsedContentMap);
                }

                Thread.sleep(1000 * 3);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // processParsedContentMapList(parsedContentMapList, writer);
    }

    private static String generateUrl(int pageNumber) {
        String baseUrl = "http://12366.cqsw.gov.cn:6001/essearch/api/search?keyword=%E4%BC%81%E4%B8%9A%E6%89%80%E5%BE%97%E7%A8%8E&keyWordsRange=titleOrContent&kssj=%22%22&jssj=%22%22&sort=%22%22&currentPage=";
        String targetUrl = baseUrl + pageNumber;

        return targetUrl;
    }

    private static String getHttpContent(String url) throws ClientProtocolException, IOException {
        if (null == url || url.isEmpty() || !url.contains("http")) {
            return null;
        }

        HttpGet get = new HttpGet(url);

        HttpClient client = HttpClients.createDefault();

        HttpResponse response = client.execute(get);
        HttpEntity entity = response.getEntity();
        String httpContent = EntityUtils.toString(entity, "UTF-8");

        client = null;
        get = null;
        return httpContent;
    }

    private static List<String> extractSubUrl(String httpContent) throws JSONException {
        if (null == httpContent || httpContent.isEmpty()) {
            return null;
        }

        List<String> urlList = new ArrayList<String>();

        JSONObject obj = new JSONObject(httpContent);
        String dataString = obj.getString("data");
        JSONArray data = new JSONArray(dataString);

        if (data == null || data.length() == 0) {
            return null;
        }

        for (int i = 0; i < data.length(); i++) {
            String urlString = (String) ((JSONObject) data.get(i)).get("docpuburl");

            urlList.add(urlString);
        }

        return urlList;
    }

    private static Map<String, String> extractContent(String url, String keyword)
            throws ClientProtocolException, IOException {
        if (null == url || url.isEmpty()) {
            return null;
        }

        String httpContent = getHttpContent(url);

        if (null == httpContent || httpContent.isEmpty() || null == keyword || keyword.isEmpty()
                || !httpContent.contains(keyword)) {
            return null;
        }

        Map<String, String> parsedContentMap = new HashMap<String, String>();
        parsedContentMap.put("keyword", keyword);
        parsedContentMap.put("url", url);

        if (httpContent.contains("%")) {
            List<String> percentageList = extractPercentage(httpContent);
            if (null == percentageList||percentageList.isEmpty()) {
                parsedContentMap.put("value", "");
            } else {
                StringBuilder sb = new StringBuilder();
                for (String percentage : percentageList) {
                    sb.append(percentage + " ");
                }

                parsedContentMap.put("value", sb.substring(0, sb.length()).toString());
            }
        } else {
            parsedContentMap.put("value", "");
        }

        return parsedContentMap;
    }

    private static List<String> extractPercentage(String content) {
        if (null == content || content.isEmpty()) {
            return null;
        }

        List<String> percentageList = new ArrayList<String>();

        String patterString = "[0-9]+\\.[0-9]+%";
        Pattern pattern = Pattern.compile(patterString);
        Matcher matcher = pattern.matcher(content);

        while (matcher.find()) {
            percentageList.add(matcher.group());
        }

        return percentageList;
    }

    private static void printMap(Map<String, String> map) {
        if (null == map || map.isEmpty()) {
            System.out.println("map is null or empty.\n");
            return;
        }

        for (Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println("\n");
    }

    private static void saveMap(Map<String, String> map, BufferedWriter writer) throws IOException {
        if (null == map || map.isEmpty() || null == writer) {
            return;
        }

        writer.write(map.get("keyword") + "," + map.get("url") + "," + map.get("value"));
        writer.newLine();

        writer.flush();
    }

    private static void processParsedContentMapList(List<Map<String, String>> parsedContentMapList,
            BufferedWriter writer) throws IOException {
        if (null == parsedContentMapList || parsedContentMapList.isEmpty() || null == writer) {
            return;
        }

        for (Map<String, String> map : parsedContentMapList) {
            if (null == map || map.isEmpty()) {
                continue;
            }

            saveMap(map, writer);
        }
    }
}