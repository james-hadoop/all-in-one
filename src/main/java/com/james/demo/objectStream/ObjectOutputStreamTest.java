package com.james.demo.objectStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class ObjectOutputStreamTest {
    public static void main(String[] args) throws IOException {
        File file = new File(Conf.FILE_PATH);
        file.createNewFile();

        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));

        Map<String, Object> mapParameter = new HashMap<String, Object>();

        Person person = new Person("James", 18, 3.98);
        mapParameter.put("person", person);
        oos.writeObject(mapParameter);

        oos.close();
    }
}
