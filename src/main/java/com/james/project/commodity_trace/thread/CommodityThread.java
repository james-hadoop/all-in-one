package com.james.project.commodity_trace.thread;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.james.common.service.IEmailService;
import com.james.common.service.impl.EmailServiceImpl;
import com.james.project.commodity_trace.config.CommodityTraceConfig;

@SuppressWarnings("deprecation")
public class CommodityThread extends Thread {
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
        final IEmailService emailService = new EmailServiceImpl();

        // timer
        Timer myTimer = new Timer();

        myTimer.schedule(new TimerTask() {
            // cycle flag
            boolean bCycle = true;

            @SuppressWarnings("resource")
            public void run() {
                try {
                    HttpClient httpClient = new DefaultHttpClient();

                    HttpGet httpGet = new HttpGet(url);

                    HttpResponse response = httpClient.execute(httpGet);
                    System.out.println(response.getStatusLine().getStatusCode());

                    HttpEntity httpEntity = response.getEntity();
                    System.out.println(EntityUtils.toString(httpEntity));

                    InputStream is = httpEntity.getContent();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                    String line = null;
                    String price = null;
                    double doublePrice = 0.0;

                    while (null != (line = reader.readLine())) {
                        if (line.contains(unit) && line.contains(config.getPriceMark())) {
                            System.out.println(line);

                            price = interceptPrice(line, unit, config.getPriceEndMark());

                            break;
                        }
                    }

                    doublePrice = convertPriceFromStringToDouble(price, config.getComma(), config.getDot());

                    if (isAlert(doublePrice, Double.parseDouble(threshold))) {
                        System.out.println(doublePrice + " : " + threshold + " --> ALERT");

                        if (true == bCycle) {
                            emailService.sendEmail(mailList, "Commodity Trace", "<h3>Commodity:<br></h3><a href=" + url
                                    + ">" + url + "</a><br>  current price is= " + doublePrice
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

    private String interceptPrice(String priceHtml, String priceMark, String priceEndMark) {
        if (null == priceHtml || 0 == priceHtml.length() || null == priceMark || 0 == priceMark.length()
                || null == priceEndMark || 0 == priceEndMark.length()) {
            return null;
        }

        int indexBegin = priceHtml.indexOf(priceMark);
        int indexEnd = priceHtml.indexOf(priceEndMark);
        if (-1 == indexBegin || -1 == indexEnd) {
            return null;
        }

        String price = priceHtml.substring(indexBegin + 4, indexEnd);

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
