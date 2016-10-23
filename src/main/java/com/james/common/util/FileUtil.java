package com.james.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {
    public static void importDataIntoFile(String path, List<byte[]> listByte) throws IOException {
        File file = new File(path);
        file.createNewFile();

        FileOutputStream fos = new FileOutputStream(file);

        for (byte[] data : listByte) {
            fos.write(data);
        }

        fos.close();
    }

    public static List<byte[]> audioFileToByteList(String path, int size) throws IOException {
        List<byte[]> listBuffer = new ArrayList<byte[]>();

        FileInputStream fis = new FileInputStream(path);

        int len = size;
        while (len == size) {
            byte[] data = new byte[size];
            len = fis.read(data);
            listBuffer.add(data);
        }

        fis.close();

        return listBuffer;
    }

    public static boolean deleteFolder(File folder) {
        return deleteFolderContents(folder) && folder.delete();
    }

    public static boolean deleteFolderContents(File folder) {
        File[] files = folder.listFiles();
        for (File file : files) {
            if (file.isFile()) {
                if (!file.delete()) {
                    return false;
                }
            } else {
                if (!deleteFolder(file)) {
                    return false;
                }
            }
        }
        return true;
    }
}
