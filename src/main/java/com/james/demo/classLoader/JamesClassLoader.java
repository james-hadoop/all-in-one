package com.james.demo.classLoader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class JamesClassLoader extends ClassLoader {
    public static void main(String[] args)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
            SecurityException, IllegalArgumentException, InvocationTargetException {
        JamesClassLoader cl = new JamesClassLoader(Thread.currentThread().getContextClassLoader(),
                "/home/james/workspace/all-in-one/target/classes/com/james/demo/classLoader");

        Class<?> clazz = cl.loadClass("com.james.demo.classLoader.Animal");

        Animal animal = (Animal) clazz.newInstance();
        animal.say();
    }

    private String path = "/home/james/workspace/all-in-one/target/classes/com/james/demo/classLoader";

    public JamesClassLoader(String path) {
        this.path = path;
    }

    public JamesClassLoader(ClassLoader parent, String path) {
        super(parent);
        this.path = path;
    }

    @Override
    public Class<?> findClass(String name) {
        byte[] data = loadClassData(name);
        return defineClass(name, data, 0, data.length);
    }

    private byte[] loadClassData(String name) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File(path + name + ".class"));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int b;
            while ((b = fis.read()) != -1) {
                baos.write(b);
            }
            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != fis) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return null;
    }
}
