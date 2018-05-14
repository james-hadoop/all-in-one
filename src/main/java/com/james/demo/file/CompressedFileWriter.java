package com.james.demo.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.james.demo.file.gzip.GZipUtils;

public class CompressedFileWriter {
    public static final String GZIP_ENCODE_UTF_8 = "UTF-8";  
    
    public static final String GZIP_ENCODE_ISO_8859_1 = "ISO-8859-1";  
    
    public static void main(String[] args) throws Exception {
        //readInputFile("pom.xml");
        
        GZipUtils.compress("GZipFolder");

        // compressGzipFile("data/a.gz", "output.txt");
    }

    public static void compressGzipFile(String path, String encoding,String output) {
        if (null == path || path.isEmpty()) {
            return;
        }
        
        
        
    }

    public static void readInputFile(String path) throws IOException {
        if (null == path || path.isEmpty()) {
            return;
        }

        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception e) {

        } finally {
            if (null != br) {
                br.close();
            }
        }
    }
    
    public static void importDataIntoFile(String path, List<byte[]> listByte) throws IOException {
        File file = new File(path);
        file.createNewFile();

        FileOutputStream fos = new FileOutputStream(file);

        for (byte[] data : listByte) {
            fos.write(data);
        }

        fos.close();
    }
    
}
