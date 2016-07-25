package com.james.demo.strings;

public class StringUtil {
	public static String getNameFromUserWithDep(String userWithDep){
		return userWithDep.substring(0,userWithDep.indexOf("("));
	}

	public static String getDepFromUserWithDep(String userWithDep){
		return userWithDep.substring(userWithDep.indexOf("(")+1,userWithDep.indexOf(")"));
	}
	
	public static void main(String[] args){
		String str="James(Data)";
		System.out.println(StringUtil.getNameFromUserWithDep(str));
		System.out.println(StringUtil.getDepFromUserWithDep(str));
	}
}
