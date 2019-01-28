package com.james.temp;

public class SqlFunctionUtil {

	public static void main(String[] args) {
		String s1 = "r_t_a.SUM(MAX(r_a_a))";
		String s2 = "r_t_a.MAX(r_a_a)";
		String s3 = "(MAX(r_a_a))";
		String s4 = "nvl(a.h,b.i)";
		String s5 = "hello";
		String s6 = "nvl(a,b)";

		String sqlFunction = s6;

		System.out.println("before: " + sqlFunction);
		System.out.println("after:  " + removeSqlFunctionName(sqlFunction));
	}

	/*
	 * hello -> hello
	 * 
	 * r_t_a.SUM(MAX(r_a_a)) -> r_t_a.r_a_a
	 * 
	 * nvl(a,b) -> a,b
	 * 
	 */

	public static String removeSqlFunctionName(String sqlFunction) {
		if (null == sqlFunction || 0 == sqlFunction.length()) {
			return null;
		}

		if (!sqlFunction.contains("(")) {
			return sqlFunction;
		}

		if (sqlFunction.contains(".")) {

		}

		int dotIndex = sqlFunction.indexOf(".");

		int begin = sqlFunction.lastIndexOf("(");

		int end = sqlFunction.indexOf(")");

		String ret = sqlFunction.substring(begin + 1, end);

		return ret;
	}

}
