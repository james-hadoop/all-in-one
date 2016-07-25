package com.james.demo.objectStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

public class ObjectInputStreamTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        File file = new File(Conf.FILE_PATH);
        file.createNewFile();

        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
        @SuppressWarnings("unchecked")
        Map<String, Object> mapParameter = (Map<String, Object>) ois.readObject();

        Person person = (Person) mapParameter.get("person");
        System.out.println(person.getName() + " " + person.getAge() + " " + person.getGpa());

        ois.close();
    }
}
