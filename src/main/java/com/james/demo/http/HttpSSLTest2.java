package com.james.demo.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class HttpSSLTest2 {
    private final static String url = "https://www.amazon.de/Aptamil-Kindermilch-Probiergr%C3%B6%C3%9Fe-Jahr-Pack/dp/B00BSNACBU/ref=sr_1_2?ie=UTF8&qid=1472697117&sr=8-2&keywords=aptamil++1%2B";

    public static void main(String[] args) throws URISyntaxException, ClientProtocolException, IOException {
        CloseableHttpClient httpClient = createSSLClientDefault();
        HttpGet get = new HttpGet();
        get.setURI(new URI(url));
        CloseableHttpResponse response = httpClient.execute(get);
        //System.out.println(response.getStatusLine().getStatusCode());

        HttpEntity httpEntity = response.getEntity();
        String responseContent = EntityUtils.toString(httpEntity);
        // System.out.println(responseContent);

        Document doc = Jsoup.parse(responseContent);

        Element infoTable = doc.getElementsByAttributeValue("class", "a-color-price").first();
        System.out.println("jsoup is :" + infoTable.text().trim());
    }

    public static CloseableHttpClient createSSLClientDefault() {
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(java.security.cert.X509Certificate[] arg0, String arg1) throws java.security.cert.CertificateException {
                    return true;
                }
            }).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext);
            return HttpClients.custom().setSSLSocketFactory(sslsf).build();

        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        }
        return HttpClients.createDefault();
    }
}
