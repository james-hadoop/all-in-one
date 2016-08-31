package com.james.demo.http;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
    private final static String url = "https://www.amazon.de/dp/B00BSNACBU/ref=cm_sw_r_other_apa_I2ZTxb2HH0V4H?from=singlemessage&isappinstalled=0";

    public static void main(String[] args) throws URISyntaxException, ClientProtocolException, IOException {
        CloseableHttpClient httpClient = createSSLClientDefault();
        HttpGet get = new HttpGet();
        get.setURI(new URI(url));
        CloseableHttpResponse response = httpClient.execute(get);
        System.out.println(response.getStatusLine().getStatusCode());

        HttpEntity httpEntity = response.getEntity();
        String responseContent=EntityUtils.toString(httpEntity);
        System.out.println(responseContent);

        InputStream is = httpEntity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        StringBuilder sb = new StringBuilder();
        try {
            String line = null;
            while (null != (line = reader.readLine())) {
                // System.out.println(line);
                if (line.contains("a-color-price")) {
                    sb.append(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            System.out.println();
            
            File file =new File("clawer.get");
            file.createNewFile();
            PrintWriter pw=new PrintWriter(file);
            pw.write(sb.toString());
            pw.close();
            
            Document doc = Jsoup.parse(responseContent);
            
            Element infoTable = doc.getElementsByAttributeValue("class", "a-color-price").first();
            System.out.println("jsoup is :" + infoTable);
            System.out.println("jsoup is :" + infoTable.text().trim());

            reader.close();
            is.close();
        }
    }

    public static CloseableHttpClient createSSLClientDefault() {
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(java.security.cert.X509Certificate[] arg0, String arg1)
                        throws java.security.cert.CertificateException {
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
