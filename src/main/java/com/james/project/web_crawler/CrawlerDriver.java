package com.james.project.web_crawler;

import java.util.List;
import java.util.Map;

public class CrawlerDriver {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    private static String generateUrl(int pageNumber) {
        String baseUrl = "http://12366.cqsw.gov.cn:6001/essearch/api/search?keyword=%E4%BC%81%E4%B8%9A%E6%89%80%E5%BE%97%E7%A8%8E&keyWordsRange=titleOrContent&kssj=%22%22&jssj=%22%22&sort=%22%22&currentPage=";
        String targetUrl = baseUrl + pageNumber;

        return targetUrl;
    }

    private static String getHttpContent(String url) {
        // TODO
        return null;
    }

    private static List<String> extractSubUrl(String newsSummary) {
        // TODO
        return null;
    }

    private static List<Map<String, Object>> extractContent(String targetContentKey) {
        // TODO
        return null;
    }
}
