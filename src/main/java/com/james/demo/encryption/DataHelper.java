package com.james.demo.encryption;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class DataHelper {
	public static String getMD5Str(String str) {
		MessageDigest messageDigest = null;

		try {
			messageDigest = MessageDigest.getInstance("MD5");

			messageDigest.reset();

			messageDigest.update(str.getBytes("UTF-8"));
		} catch (NoSuchAlgorithmException e) {
			System.out.println("NoSuchAlgorithmException caught!");
			System.exit(-1);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		byte[] byteArray = messageDigest.digest();

		StringBuffer md5StrBuff = new StringBuffer();

		for (int i = 0; i < byteArray.length; i++) {
			if (Integer.toHexString(0xFF & byteArray[i]).length() == 1)
				md5StrBuff.append("0").append(Integer.toHexString(0xFF & byteArray[i]));
			else
				md5StrBuff.append(Integer.toHexString(0xFF & byteArray[i]));
		}

		return md5StrBuff.toString();
	}

	public static boolean isNumber(String str) {
		boolean ret = false;
		if (isNullOrEmpty(str)) {
			ret = false;
		} else {
			Pattern pattern = Pattern.compile("[0-9]*");
			ret = pattern.matcher(str).matches();
		}
		return ret;
	}

	public static Long getLong(Object obj) {
		if (obj == null || false == isNumber(obj + ""))
			return 0L;
		return Long.valueOf(obj + "");
	}

	public static int getInt(Object obj) {
		if (obj == null || false == isNumber(obj + ""))
			return 0;
		return Integer.valueOf(obj + "");
	}

	public static boolean isNullOrEmpty(String input) {
		if (input == null || input.length() <= 0) {
			return true;
		}
		return false;
	}

	public static String StringJoin(String connector, String[] items) {
		StringBuilder ret = new StringBuilder();
		for (int i = 0; i < items.length; i++) {
			if (i == items.length - 1) {
				ret.append(items[i]);
			} else {
				ret.append(items[i]).append(connector);
			}
		}
		return ret.toString();
	}
	
	public static String StringJoin(String connector, List<String> items) {
		StringBuilder ret = new StringBuilder();
		for (String string : items) {
			ret.append(string).append(connector);
		}
		ret.deleteCharAt(ret.lastIndexOf(connector));
		
		return ret.toString();
	}
	
	public static String StringJoin(String connector, Set<String> items) {
		StringBuilder ret = new StringBuilder();
		for (String string : items) {
			ret.append(string).append(connector);
		}
		ret.deleteCharAt(ret.lastIndexOf(connector));
		
		return ret.toString();
	}

	public static String StringJoin(String connector, Collection<String> values) {
		StringBuilder ret = new StringBuilder();
		for (String string : values) {
			ret.append(string).append(connector);
		}
		ret.deleteCharAt(ret.lastIndexOf(connector));
		
		return ret.toString();
	}
}
