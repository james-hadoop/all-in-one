package com.james.demo.classLoader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class JamesClassLoader2 extends ClassLoader {
	private String path = "/home/james/workspace/all-in-one/target/classes/com/james/demo/classLoader";

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

	public class Cat {
		public void say() {
			System.out.println("Hello Cat");
		}
	}

}
