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

        Document doc = Jsoup.connect("http://list.mp3.baidu.com/top/singer/A.html").get();
        Element singerListDiv = doc.getElementsByAttributeValue("class", "content").first();
        Elements links = singerListDiv.getElementsByTag("a");

        for (Element link : links) {
            String linkHref = link.attr("href");
            String linkText = link.text().trim();
            System.out.println(linkHref);
        }
        System.out.println("------------------------------------------------------------------------------");

        doc = Jsoup.connect("http://www.nongli.com/item4/index.asp?dt=2012-03-03").get();
        Element infoTable = doc.getElementsByAttributeValue("class", "table002").first();
        Elements tableLineInfos = infoTable.select("tr");
        for (Element lineInfo : tableLineInfos) {
            String lineInfoContent = lineInfo.select("td").last().text().trim();
            System.out.println("jsoup is :" + lineInfoContent);
        }
        System.out.println("------------------------------------------------------------------------------");

        File input = new File("oschina.html");
        input.createNewFile();
        doc = Jsoup.parse(input, "UTF-8", "http://www.oschina.net/");
        links = doc.select("a[href]"); // 链接
        Elements pngs = doc.select("img[src$=.png]"); // 所有 png 的图片
        Element masthead = doc.select("div.masthead").first();// div with
                                                              // class=masthead
        Elements resultLinks = doc.select("h3.r > a");
        for (Element e : resultLinks) {
            System.out.println(e);
        }
        System.out.println("------------------------------------------------------------------------------");

        doc = Jsoup.parse(input, "UTF-8", "http://www.oschina.net/");
        links = doc.select("a[href]"); // 链接
        pngs = doc.select("img[src$=.png]"); // 所有 png 的图片
        masthead = doc.select("div.masthead").first();// div with class=masthead
        resultLinks = doc.select("h3.r > a");
    }
}
