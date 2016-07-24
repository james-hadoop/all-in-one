package com.james.http.trace.driver;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import com.james.common.conf.CommonConf;
import com.james.common.service.IEmailService;
import com.james.common.service.impl.EmailServiceImpl;
import com.james.http.trace.conf.HttpTraceConf;

@SuppressWarnings("deprecation")
public class CommodityTrace {
    public static void main(String[] args) {
        startTraceTimer();
    }

    public static String interceptPrice(String priceHtml, String priceMark, String priceEndMark) {
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

    public static double convertPriceFromStringToDouble(String price, String comma, String dot) {
        if (null == price || 0 == price.length()) {
            return 0.0;
        }

        price = price.replace(comma, dot);

        return Double.parseDouble(price);
    }

    public static boolean isAlert(double price, double threshold) {
        if (price < threshold) {
            return true;
        }

        return false;
    }

    private static void startTraceTimer() {
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
                    DefaultHttpClient httpClient = new DefaultHttpClient();

                    HttpGet httpGet = new HttpGet(HttpTraceConf.TRACE_URL);

                    HttpResponse response = httpClient.execute(httpGet);
                    // System.out.println(response.getStatusLine().getStatusCode());

                    HttpEntity httpEntity = response.getEntity();
                    // System.out.println(EntityUtils.toString(httpEntity));

                    InputStream is = httpEntity.getContent();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                    String line = null;
                    String price = null;
                    double doublePrice = 0.0;

                    while (null != (line = reader.readLine())) {
                        if (line.contains(HttpTraceConf.UNIT) && line.contains(HttpTraceConf.PRICE_MARK)) {
                            // System.out.println(line);

                            price = interceptPrice(line, HttpTraceConf.UNIT, HttpTraceConf.PRICE_END_MARK);

                            break;
                        }
                    }

                    doublePrice = convertPriceFromStringToDouble(price, HttpTraceConf.COMMA, HttpTraceConf.DOT);

                    if (isAlert(doublePrice, HttpTraceConf.PRICE_THRESHOLD)) {
                        System.out.println(doublePrice + " : " + HttpTraceConf.PRICE_THRESHOLD + " --> ALERT");

                        if (true == bCycle) {
                            emailService.sendEmail(CommonConf.MAIL_RECEIVE_BOX, "Commodity Trace",
                                    "<h3>Commodity:<br></h3><a href=" + HttpTraceConf.TRACE_URL + ">"
                                            + HttpTraceConf.TRACE_URL + "</a><br>  current price is= " + doublePrice
                                            + ", which is lower than threshold price=" + HttpTraceConf.PRICE_THRESHOLD);
                        }

                        bCycle = false;
                    } else {
                        System.out.println(doublePrice + " : " + HttpTraceConf.PRICE_THRESHOLD + " --> NOT ALERT");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, HttpTraceConf.HTTP_TRACE_INTERVAL + 1, HttpTraceConf.HTTP_TRACE_INTERVAL * 60 * 60 * 1000);
    }
}
