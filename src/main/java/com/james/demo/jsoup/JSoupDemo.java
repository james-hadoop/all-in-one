package com.james.demo.jsoup;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class JSoupDemo {
    public static void main(String[] args) throws IOException {
        Document doc = Jsoup.connect(
                "http://12366.cqsw.gov.cn:6001/essearch/essearch/pages/search.html?keyword=%E4%BC%81%E4%B8%9A%E6%89%80%E5%BE%97%E7%A8%8E")
                .get();

        Elements pagers = doc.getElementsByAttributeValue("class", "tablepage");
        if(null==pagers) {
            System.out.println("pagers is null");
        }
        
        for (Element pager : pagers) {
        System.out.println("pager:\n\t" + pager.text());
        }
    }
}
