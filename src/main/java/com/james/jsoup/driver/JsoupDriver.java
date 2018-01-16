package com.james.jsoup.driver;

import java.io.File;
import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class JsoupDriver {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello Jsoup!");

        Document doc = Jsoup.connect("http://12366.cqsw.gov.cn:6001/essearch/essearch/pages/search.html?keyword=%E4%BC%81%E4%B8%9A%E6%89%80%E5%BE%97%E7%A8%8E").get();
        Element singerListDiv = doc.getElementsByAttributeValue("class", "content").first();
        Elements links = singerListDiv.getElementsByTag("a");

        for (Element link : links) {
            String linkHref = link.attr("href");
            String linkText = link.text().trim();
            System.out.println(linkHref);
        }
        System.out.println("------------------------------------------------------------------------------");

        doc = Jsoup.connect("http://12366.cqsw.gov.cn:6001/essearch/essearch/pages/search.html?keyword=%E4%BC%81%E4%B8%9A%E6%89%80%E5%BE%97%E7%A8%8E").get();
        Element infoTable = doc.getElementsByAttributeValue("class", "table002").first();
        Elements tableLineInfos = infoTable.select("tr");
        for (Element lineInfo : tableLineInfos) {
            String lineInfoContent = lineInfo.select("td").last().text().trim();
            System.out.println("jsoup is :" + lineInfoContent);
        }
        System.out.println("------------------------------------------------------------------------------");

        File input = new File("oschina.html");
        input.createNewFile();
        doc = Jsoup.parse(input, "UTF-8", "http://12366.cqsw.gov.cn:6001/essearch/essearch/pages/search.html?keyword=%E4%BC%81%E4%B8%9A%E6%89%80%E5%BE%97%E7%A8%8E");
        links = doc.select("a[href]"); // 链接
        Elements pngs = doc.select("img[src$=.png]"); // 所有 png 的图片
        Element masthead = doc.select("div.masthead").first();// div with
                                                              // class=masthead
        Elements resultLinks = doc.select("h3.r > a");
        for (Element e : resultLinks) {
            System.out.println(e);
        }
        System.out.println("------------------------------------------------------------------------------");

        doc = Jsoup.parse(input, "UTF-8", "http://12366.cqsw.gov.cn:6001/essearch/essearch/pages/search.html?keyword=%E4%BC%81%E4%B8%9A%E6%89%80%E5%BE%97%E7%A8%8E");
        links = doc.select("a[href]"); // 链接
        pngs = doc.select("img[src$=.png]"); // 所有 png 的图片
        masthead = doc.select("div.masthead").first();// div with class=masthead
        resultLinks = doc.select("h3.r > a");
    }
}
