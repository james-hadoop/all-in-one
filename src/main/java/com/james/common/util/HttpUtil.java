package com.james.common.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtil {
//    public static abstract class HttpResponseHandler {
//        public abstract void handle(HttpResponse response) throws Throwable;
//
//        public abstract void exceptionCaught(HttpRequestBase req, Throwable ex);
//
//        public void httpGet(String url) {
//            httpGet(SimpleHttpClient.default_http_client, url);
//        }
//
//        public void httpGet(HttpClient client, String url) {
//            HttpGet httpGet = null;
//            long before = System.currentTimeMillis();
//            Throwable ex = null;
//            try {
//                httpGet = new HttpGet(url);
//                HttpResponse response = null;
//                try {
//                    response = client.execute(httpGet);
//                } catch (Throwable e1) {
//                    exceptionCaught(httpGet, e1);
//                    throw e1;
//                }
//                handle(response);
//            } catch (Throwable e) {
//                ex = e;
//            } finally {
//                long span = System.currentTimeMillis() - before;
//                if (ex != null) {
//                    logger.error("httpGet  -ERROR- [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url,
//                            ex });
//                } else if (span >= slow_threshold) {
//                    logger.warn("httpGet  -SLOW-  [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url });
//                } else {
//                    logger.debug("httpGet  -OK-    [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url });
//                }
//                if (null != httpGet) {
//                    httpGet.abort();
//                }
//            }
//        }
//
//        public void httpPost(String url, String postContent) {
//            httpPost(SimpleHttpClient.default_http_client, url, postContent, CharsetTools.UTF_8.name());
//        }
//
//        public void httpPost(HttpClient client, String url, String postContent, String defaultCharset) {
//            HttpPost httpPost = null;
//            long before = System.currentTimeMillis();
//            Throwable ex = null;
//            try {
//                httpPost = new HttpPost(url);
//                HttpEntity entity = new StringEntity(postContent, defaultCharset);
//                httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
//                httpPost.setEntity(entity);
//                HttpResponse response = null;
//                try {
//                    response = client.execute(httpPost);
//                } catch (Throwable e1) {
//                    exceptionCaught(httpPost, e1);
//                    throw e1;
//                }
//                handle(response);
//            } catch (Throwable e) {
//                ex = e;
//            } finally {
//                long span = System.currentTimeMillis() - before;
//                if (ex != null) {
//                    logger.error("httpPost -ERROR- [{}]url:{},postContent:{}",
//                            new Object[] { HumanReadableUtils.timeSpan(span), url, postContent, ex });
//                } else if (span >= slow_threshold) {
//                    logger.warn("httpPost -SLOW-  [{}]url:{},postContent:{}",
//                            new Object[] { HumanReadableUtils.timeSpan(span), url, postContent });
//                } else {
//                    logger.debug("httpPost -OK-  [{}]url:{},postContent:{}",
//                            new Object[] { HumanReadableUtils.timeSpan(span), url, postContent });
//                }
//                if (null != httpPost) {
//                    httpPost.abort();
//                }
//            }
//        }
//    }
//
//    private static final Logger logger = LoggerFactory.getLogger(HttpClientUtils.class);
//    private static final int slow_threshold = 1000;
//
//    public static int httpHead(String url) {
//        return httpHead(url, CharsetTools.UTF_8.name());
//    }
//
//    public static int httpHead(String url, String defaultCharset) {
//        return httpHead(SimpleHttpClient.default_http_client, url, defaultCharset);
//    }
//
//    public static int httpHead(HttpClient client, String url, String defaultCharset) {
//        HttpGet httpGet = null;
//        StatusLine sl = null;
//        long before = System.currentTimeMillis();
//        Exception ex = null;
//        try {
//            httpGet = new HttpGet(url);
//            HttpResponse response = client.execute(httpGet);
//            sl = response.getStatusLine();
//
//            return sl.getStatusCode();
//        } catch (Exception e) {
//            ex = e;
//        } finally {
//            long span = System.currentTimeMillis() - before;
//            if (ex != null) {
//                logger.error("httpHead -ERROR- [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url, ex });
//            } else if (span >= slow_threshold) {
//                logger.warn("httpHead -SLOW-  [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url });
//            } else {
//                logger.debug("httpHead -OK-    [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url });
//            }
//            if (null != httpGet) {
//                httpGet.abort();
//            }
//        }
//        return Integer.MIN_VALUE;
//    }
//
//    public static String httpGet(HttpClient client, String url, String defaultCharset) {
//        return httpGet(client, url, defaultCharset, null);
//    }
//
//    public static String httpGet(HttpClient client, String url, String defaultCharset, List<Header> headers) {
//        HttpGet httpGet = null;
//        String result = "";
//        long before = System.currentTimeMillis();
//        Throwable ex = null;
//        try {
//            httpGet = new HttpGet(url);
//
//            if (headers != null && headers.size() > 0) {
//                for (Header header : headers) {
//                    httpGet.setHeader(header);
//                }
//            }
//
//            HttpResponse response = client.execute(httpGet);
//            HttpEntity entity = response.getEntity();
//            result = EntityUtils.toString(entity, defaultCharset);
//            return result;
//        } catch (Throwable e) {
//            ex = e;
//        } finally {
//            long span = System.currentTimeMillis() - before;
//            if (ex != null) {
//                logger.error("httpGet  -ERROR- [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url, ex });
//            } else if (span >= slow_threshold) {
//                logger.warn("httpGet  -SLOW-  [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url });
//            } else {
//                logger.debug("httpGet  -OK-    [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url });
//            }
//            if (null != httpGet) {
//                httpGet.abort();
//            }
//        }
//        return null;
//    }
//
//    public static byte[] httpGet(HttpClient client, String url) {
//        HttpGet httpGet = null;
//        byte[] result = null;
//        long before = System.currentTimeMillis();
//        Exception ex = null;
//        try {
//            httpGet = new HttpGet(url);
//            HttpResponse response = client.execute(httpGet);
//            HttpEntity entity = response.getEntity();
//            if (entity.isStreaming()) {
//                result = EntityUtils.toByteArray(entity);
//            }
//            return result;
//        } catch (Exception e) {
//            ex = e;
//        } finally {
//            long span = System.currentTimeMillis() - before;
//            if (ex != null) {
//                logger.error("httpGet  -ERROR- [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url, ex });
//            } else if (span >= slow_threshold) {
//                logger.warn("httpGet  -SLOW-  [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url });
//            } else {
//                logger.debug("httpGet  -OK-    [{}]url:{}", new Object[] { HumanReadableUtils.timeSpan(span), url });
//            }
//            if (null != httpGet) {
//                httpGet.abort();
//            }
//        }
//        return null;
//    }
//
//    public static byte[] httpGetByteArray(String url) {
//        return httpGet(SimpleHttpClient.default_http_client, url);
//    }
//
//    public static String httpGet(String url) {
//        return httpGet(url, CharsetTools.UTF_8.name());
//    }
//
//    public static String httpGet(String url, Map<String, Object> params) throws IOException {
//        String finalUrl = buildGetUrl(url, buildQuery(params, CharsetTools.UTF_8.name())).toString();
//        return httpGet(finalUrl, CharsetTools.UTF_8.name());
//    }
//
//    private static URL buildGetUrl(String strUrl, String query) throws IOException {
//        URL url = new URL(strUrl);
//        if (StringUtils.isBlank(query)) {
//            return url;
//        }
//
//        if (StringUtils.isBlank(url.getQuery())) {
//            if (strUrl.endsWith("?")) {
//                strUrl = strUrl + query;
//            } else {
//                strUrl = strUrl + "?" + query;
//            }
//        } else {
//            if (strUrl.endsWith("&")) {
//                strUrl = strUrl + query;
//            } else {
//                strUrl = strUrl + "&" + query;
//            }
//        }
//
//        return new URL(strUrl);
//    }
//
//    public static String buildQuery(Map<String, Object> params, String charset) throws IOException {
//        if (params == null || params.isEmpty()) {
//            return null;
//        }
//        StringBuilder query = new StringBuilder();
//        Set<Map.Entry<String, Object>> entries = params.entrySet();
//        boolean hasParam = false;
//        for (Map.Entry<String, Object> entry : entries) {
//            String name = entry.getKey();
//            String value = entry.getValue().toString();
//            if (StringUtils.isNotBlank(name)) {
//                if (hasParam) {
//                    query.append("&");
//                } else {
//                    hasParam = true;
//                }
//                query.append(name).append("=").append(URLEncoder.encode(value, charset));
//            }
//        }
//        return query.toString();
//    }
//
//    public static String httpGet(String url, List<Header> headers) {
//        return httpGet(SimpleHttpClient.default_http_client, url, CharsetTools.UTF_8.name(), headers);
//    }
//
//    public static String httpGet(String url, String defaultCharset) {
//        return httpGet(SimpleHttpClient.default_http_client, url, defaultCharset);
//    }
//
//    public static String httpPost(HttpClient client, String url, String postContent, String defaultCharset,
//            List<Header> headers) {
//        HttpPost httpPost = null;
//        String content = "";
//        long before = System.currentTimeMillis();
//        Throwable ex = null;
//        try {
//            httpPost = new HttpPost(url);
//            HttpEntity entity = new StringEntity(postContent, defaultCharset);
//            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
//            httpPost.setEntity(entity);
//
//            if (headers != null && headers.size() > 0) {
//                for (Header header : headers) {
//                    httpPost.setHeader(header);
//                }
//            }
//
//            HttpResponse response = client.execute(httpPost);
//            content = EntityUtils.toString(response.getEntity(), defaultCharset);
//            return content;
//        } catch (Throwable e) {
//            ex = e;
//        } finally {
//            long span = System.currentTimeMillis() - before;
//            if (ex != null) {
//                logger.error("httpPost -ERROR- [{}]url:{},postContent:{}",
//                        new Object[] { HumanReadableUtils.timeSpan(span), url, postContent, ex });
//            } else if (span >= slow_threshold) {
//                logger.warn("httpPost -SLOW-  [{}]url:{},postContent:{}",
//                        new Object[] { HumanReadableUtils.timeSpan(span), url, postContent });
//            } else {
//                logger.debug("httpPost -OK-  [{}]url:{},postContent:{}",
//                        new Object[] { HumanReadableUtils.timeSpan(span), url, postContent });
//            }
//            if (null != httpPost) {
//                httpPost.abort();
//            }
//        }
//        return null;
//    }
//
//    public static String httpPost(HttpClient client, String url, String postContent, String defaultCharset) {
//        return httpPost(client, url, postContent, defaultCharset, null);
//    }
//
//    public static String httpPost(String url, String postContent) {
//        return httpPost(SimpleHttpClient.default_http_client, url, postContent, CharsetTools.UTF_8.name());
//    }
//
//    public static String httpPost(String url, String postContent, List<Header> headers) {
//        return httpPost(SimpleHttpClient.default_http_client, url, postContent, CharsetTools.UTF_8.name(), headers);
//    }
//
//    public static String httpPost(String url, String postContent, String defaultCharset) {
//        return httpPost(SimpleHttpClient.default_http_client, url, postContent, defaultCharset);
//    }
//
//    /**
//     * 简单重试方法
//     * 
//     * @param url
//     * @param attempts
//     * @param sleepSec
//     * @return
//     */
//    public static String httpGet(String url, int attempts, int sleepSec) {
//        for (int i = 0; i < attempts; i++) {
//            String r = httpGet(url, CharsetTools.UTF_8.name());
//            if (r != null) {
//                return r;
//            }
//            try {
//                Thread.sleep(sleepSec * 1000);
//            } catch (InterruptedException e) {
//                // e.printStackTrace();
//            }
//        }
//        return null;
//    }
//
//    public static int httpHead(String url, int attempts, int sleepSec) {
//        for (int i = 0; i < attempts; i++) {
//            int r = httpHead(url, CharsetTools.UTF_8.name());
//            if (r != Integer.MIN_VALUE) {
//                return r;
//            }
//            try {
//                Thread.sleep(sleepSec * 1000);
//            } catch (InterruptedException e) {
//                // e.printStackTrace();
//            }
//        }
//        return Integer.MIN_VALUE;
//    }
    
    public static final String DEFAULT_CHARSET = "UTF-8";

    private static final String METHOD_POST = "POST";

    private static final String METHOD_GET = "GET";
    
    private static URL buildGetUrl(String strUrl, String query) throws IOException {
        URL url = new URL(strUrl);
        if (StringUtils.isBlank(query)) {
            return url;
        }

        if (StringUtils.isBlank(url.getQuery())) {
            if (strUrl.endsWith("?")) {
                strUrl = strUrl + query;
            } else {
                strUrl = strUrl + "?" + query;
            }
        } else {
            if (strUrl.endsWith("&")) {
                strUrl = strUrl + query;
            } else {
                strUrl = strUrl + "&" + query;
            }
        }

        return new URL(strUrl);
    }

    public static String buildQuery(Map<String, String> params, String charset) throws IOException {
        if (params == null || params.isEmpty()) {
            return null;
        }
        StringBuilder query = new StringBuilder();
        Set<Map.Entry<String, String>> entries = params.entrySet();
        boolean hasParam = false;
        for (Map.Entry<String, String> entry : entries) {
            String name = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isNotBlank(name)) {
                if (hasParam) {
                    query.append("&");
                } else {
                    hasParam = true;
                }
                query.append(name).append("=").append(URLEncoder.encode(value, charset));
            }
        }
        return query.toString();
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
