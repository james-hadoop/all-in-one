package com.james.demo.classLoader;

import java.lang.reflect.InvocationTargetException;

import com.james.demo.classLoader.JamesClassLoader2.Cat;

public class ClassLoaderDriver {
	public static void main(String[] args)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
			SecurityException, IllegalArgumentException, InvocationTargetException {
		JamesClassLoader cl = new JamesClassLoader(Thread.currentThread().getContextClassLoader(),
				"/home/james/workspace/all-in-one/target/classes/com/james/demo/classLoader");

		Class<?> clazz = cl.loadClass("com.james.demo.classLoader.JamesClassLoader2$Cat");

		Cat cat = (Cat) clazz.getConstructors()[0].newInstance(new JamesClassLoader2());
		cat.say();
	}
}
