package com.james.common.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class AudioUtil {
    private static final String prefix = "http://tsn.baidu.com/text2audio?tex=";
    private static final String postfix = "&lan=zh&cuid=xxx&ctp=1&tok=24.6f15944a6841b99c0ce0a04d6da4d7ab.2592000.1442368413.282335-6072402";

    public static List<byte[]> text2Audio(String text) throws IOException {
        List<byte[]> listBuffer = new ArrayList<byte[]>();

        String url = prefix + URLEncoder.encode(new String(text.getBytes()), "utf-8") + postfix;
        URL uri = new URL(url);

        HttpURLConnection conn = null;
        conn = (HttpURLConnection) uri.openConnection();
        if (conn == null) {
            System.out.println("conn == null");
        }
        conn.connect();

        int res = conn.getResponseCode();
        if (res == 200) {
            // response code
            System.out.println("res == 200");

            // response data
            File file = new File("data/listAudio.mp3");
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);

            InputStream is = conn.getInputStream();

            int len = 0;
            do {
                byte[] buffer = new byte[4096];

                len = is.read(buffer);

                listBuffer.add(buffer);
                System.out.println("len: " + len);
                System.out.println("buffer: " + buffer.toString());

                fos.write(buffer);
            } while (len != -1);

            fos.close();
            is.close();
        }

        conn.disconnect();
        return listBuffer;
    }

    public static void importAudioIntoFile(List<byte[]> listBuffer, String file) throws IOException {
        File fileHandler = new File(file);
        fileHandler.createNewFile();
        FileOutputStream fos = new FileOutputStream(file);

        for (byte[] buffer : listBuffer) {
            fos.write(buffer);
        }

        fos.close();
    }
}