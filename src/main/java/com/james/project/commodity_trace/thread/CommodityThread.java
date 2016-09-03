package com.james.project.commodity_trace.thread;

import java.net.URI;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.james.common.config.CommonConfig;
import com.james.common.service.IEmailService;
import com.james.common.service.impl.EmailServiceImpl;
import com.james.common.util.HttpUtil;
import com.james.project.commodity_trace.config.CommodityTraceConfig;

public class CommodityThread extends Thread {
    private CommonConfig commonConfig =CommonConfig.getInstance();
    private CommodityTraceConfig config = CommodityTraceConfig.getInstance();

    private String url;
    private String threshold;
    private String unit;
    private String mailList;

    public CommodityThread() {
        this.url = config.getUrl();
        this.threshold = config.getThreshold();
        this.unit = config.getUnit();
        this.mailList = config.getMailList();

        startTraceTimer();
    }

    private void startTraceTimer() {
        // services
        final IEmailService emailService = new EmailServiceImpl(commonConfig.getMailFrom(), commonConfig.getMailHost(), commonConfig.getMailUserName(), commonConfig.getMailPassword(), commonConfig.getMailPort());

        // timer
        Timer myTimer = new Timer();

        myTimer.schedule(new TimerTask() {
            // cycle flag
            boolean bCycle = true;

            public void run() {
                try {
                    CloseableHttpClient httpClient = HttpUtil.createSSLClientDefault();
                    HttpGet get = new HttpGet();
                    get.setURI(new URI(url));
                    CloseableHttpResponse response = httpClient.execute(get);
                    // System.out.println(response.getStatusLine().getStatusCode());

                    HttpEntity httpEntity = response.getEntity();
                    String responseContent = EntityUtils.toString(httpEntity);
                    // System.out.println(responseContent);

                    Document doc = Jsoup.parse(responseContent);

                    Element element = doc.getElementsByAttributeValue("class", "a-color-price").first();
                    String price = element.text().trim();
                    price = interceptPrice(price, unit);

                    double doublePrice = 0.0;

                    doublePrice = convertPriceFromStringToDouble(price, config.getComma(), config.getDot());

                    if (isAlert(doublePrice, Double.parseDouble(threshold))) {
                        System.out.println(doublePrice + " : " + threshold + " --> ALERT");

                        if (true == bCycle) {
                            emailService.sendEmail(mailList, "Commodity Trace", "<h3>Commodity:<br></h3><a href=" + url + ">" + url + "</a><br>  current price is= " + doublePrice
                                    + ", which is lower than threshold price=" + threshold);
                        }

                        bCycle = false;
                    } else {
                        System.out.println(doublePrice + " : " + threshold + " --> NOT ALERT");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, config.getInterval() + 1, config.getInterval() * 60 * 1000);
    }

    private String interceptPrice(String priceHtml, String priceMark) {
        if (null == priceHtml || 0 == priceHtml.length() || null == priceMark || 0 == priceMark.length()) {
            return null;
        }

        int indexBegin = priceHtml.indexOf(priceMark);
        if (-1 == indexBegin) {
            return null;
        }

        String price = priceHtml.substring(indexBegin + 4, priceHtml.length());

        return price;
    }

    private double convertPriceFromStringToDouble(String price, String comma, String dot) {
        if (null == price || 0 == price.length()) {
            return 0.0;
        }

        price = price.replace(comma, dot);

        return Double.parseDouble(price);
    }

    private boolean isAlert(double price, double threshold) {
        if (price < threshold) {
            return true;
        }

        return false;
    }
}
