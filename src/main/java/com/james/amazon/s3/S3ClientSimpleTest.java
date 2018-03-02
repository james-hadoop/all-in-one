package com.james.amazon.s3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import sun.misc.BASE64Encoder;

public class S3ClientSimpleTest {

    private static final Logger logger = Logger.getLogger(S3ClientSimpleTest.class);

    static String accessKey = "";
    static String secretKey = "";
    static String bucket = "";

    public static void main(String[] args) throws Exception {
        //File localFile = new File("D:/temp/s3testfile.txt");
        // putObject(localFile);
        // getObject("scripts/auto-client-events-extraction/auto_events_hadoop2.sql",
        // "d:/temp/download", null, null, null);
        System.out.println(isExisted("scripts/1.sql"));

    }

    public static void putObject(File localFile) throws Exception {
        HttpURLConnection conn = null;
        BufferedInputStream in = null;
        BufferedOutputStream out = null;
        try {
            URL url = new URL("http://" + bucket + ".s3.amazonaws.com/" + localFile.getName());
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setDoInput(true);

            String contentMD5 = md5file(localFile);
            logger.info("ContentMD5: " + contentMD5);
            String contentType = "application/xml";
            Date date = new Date();
            String dateString = DateUtil.formatDate(date, DateUtil.PATTERN_RFC1036);
            String sign = sign("PUT", contentMD5, contentType, dateString, "/" + bucket + "/" + localFile.getName(), null);
            conn.setRequestProperty("Date", dateString);
            conn.setRequestProperty("Authorization", sign);
            conn.setRequestProperty("Content-Type", contentType);
            conn.setRequestProperty("Content-MD5", contentMD5);

            out = new BufferedOutputStream(conn.getOutputStream());
            in = new BufferedInputStream(new FileInputStream(localFile));

            byte[] buffer = new byte[1024];
            int p = 0;
            while ((p = in.read(buffer)) != -1) {
                out.write(buffer, 0, p);
                out.flush();
            }

            int status = conn.getResponseCode();
            logger.info("http status: " + status);
            logger.info("after:\n" + conn.getHeaderFields());

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            close(in);
            close(out);
        }
    }

    public static File getObject(String objectName, String rootPath, Long start, Long end, String etag) {

        HttpURLConnection conn = null;
        BufferedInputStream in = null;
        BufferedOutputStream out = null;
        try {
            URL url = new URL("http://" + bucket + ".s3.amazonaws.com/" + objectName);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(true);

            String contentType = "application/xml";
            Date date = new Date();
            String dateString = DateUtil.formatDate(date, DateUtil.PATTERN_RFC1036);
            String sign = sign("GET", "", contentType, dateString, "/" + bucket + "/" + objectName, null);
            conn.setRequestProperty("Date", dateString);
            conn.setRequestProperty("Authorization", sign);
            conn.setRequestProperty("Content-Type", contentType);

            // Range 特性
            if (start != null && end != null) {
                conn.setRequestProperty("Range", "bytes=" + start + "-" + end);
            }

            // Etag 特性
            if (StringUtils.isNotBlank(etag)) {
                conn.setRequestProperty("If-None-Match", etag);
            }

            int status = conn.getResponseCode();
            logger.info("http status: " + status);
            if (status == 304) {
                // ETAG未变化，文件未变化，服务器返回空body
                logger.info("after:\n" + conn.getHeaderFields());
                return null;
            }

            in = new BufferedInputStream(conn.getInputStream());
            File localFile = new File(rootPath + "/" + objectName);
            if (!localFile.getParentFile().exists()) {
                localFile.getParentFile().mkdirs();
            }
            out = new BufferedOutputStream(new FileOutputStream(localFile, false));

            byte[] buffer = new byte[1024];
            int p = 0;
            while ((p = in.read(buffer)) != -1) {
                out.write(buffer, 0, p);
                out.flush();
            }
            logger.info("after:\n" + conn.getHeaderFields());
            return localFile;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            close(in);
            close(out);
        }
    }

    public static boolean isExisted(String objectName) {

        HttpURLConnection conn = null;
        
        try {
            URL url = new URL("http://" + bucket + ".s3.amazonaws.com/" + objectName);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(true);

            String contentType = "application/xml";
            Date date = new Date();
            String dateString = DateUtil.formatDate(date, DateUtil.PATTERN_RFC1036);
            String sign = sign("GET", "", contentType, dateString, "/" + bucket + "/" + objectName, null);
            conn.setRequestProperty("Date", dateString);
            conn.setRequestProperty("Authorization", sign);
            conn.setRequestProperty("Content-Type", contentType);

            int status = conn.getResponseCode();
            System.out.println("http status: " + status);

            return false;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            conn.disconnect();
        }
    }

    private static void close(Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * MD5文件
     * 
     * @param file
     * @return
     * @throws Exception
     */
    public static String md5file(File file) throws Exception {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
        byte[] buf = new byte[1024 * 100];
        int p = 0;
        while ((p = in.read(buf)) != -1) {
            messageDigest.update(buf, 0, p);
        }
        in.close();
        byte[] digest = messageDigest.digest();

        BASE64Encoder encoder = new BASE64Encoder();
        return encoder.encode(digest);
    }

    /**
     * 计算签名
     * 
     * @param httpVerb
     * @param contentMD5
     * @param contentType
     * @param date
     * @param resource
     * @param metas
     * @return
     */
    public static String sign(String httpVerb, String contentMD5, String contentType, String date, String resource, Map<String, String> metas) {

        String stringToSign = httpVerb + "\n" + StringUtils.trimToEmpty(contentMD5) + "\n" + StringUtils.trimToEmpty(contentType) + "\n" + date + "\n";
        if (metas != null) {
            for (Map.Entry<String, String> entity : metas.entrySet()) {
                stringToSign += StringUtils.trimToEmpty(entity.getKey()) + ":" + StringUtils.trimToEmpty(entity.getValue()) + "\n";
            }
        }
        stringToSign += resource;
        try {
            Mac mac = Mac.getInstance("HmacSHA1");
            byte[] keyBytes = secretKey.getBytes("UTF8");
            SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA1");
            mac.init(signingKey);
            byte[] signBytes = mac.doFinal(stringToSign.getBytes("UTF8"));
            String signature = encodeBase64(signBytes);
            return "AWS" + " " + accessKey + ":" + signature;
        } catch (Exception e) {
            throw new RuntimeException("MAC CALC FAILED.");
        }

    }

    private static String encodeBase64(byte[] data) {
        String base64 = new String(Base64.encodeBase64(data));
        if (base64.endsWith("\r\n"))
            base64 = base64.substring(0, base64.length() - 2);
        if (base64.endsWith("\n"))
            base64 = base64.substring(0, base64.length() - 1);

        return base64;
    }
}