package com.james.demo.jsoup;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class JSoupTest {
    public static void main(String[] args) throws IOException {
        Document doc = Jsoup.connect("http://www.nongli.com/item4/index.asp?dt=2012-03-03").get();
        Element infoTable = doc.getElementsByAttributeValue("class", "table002").first();
        Elements tableLineInfos = infoTable.select("tr");
        for (Element lineInfo : tableLineInfos) {
            String lineInfoContent = lineInfo.select("td").last().text().trim();
            System.out.println("jsoup is :" + lineInfoContent);
        }
    }
}
