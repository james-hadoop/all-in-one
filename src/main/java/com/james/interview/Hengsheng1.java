package com.james.interview;

import com.james.common.util.JamesUtil;

public class Hengsheng1 {
	public static void aMethod() throws Exception {
		try {
			throw new Exception();
		} finally {
			System.out.println("finally");
		}
	}

	public static boolean foo(char c) {
		System.out.print(c);
		return true;
	}

	public static void main(String[] args) {
		try {
			aMethod();
		} catch (Exception e) {
			System.out.println("exception");
		}
		System.out.println("finished");
		JamesUtil.printDivider();

		int i = 0;
		for (foo('A'); foo('B') && (i < 2); foo('C')) {
			i++;
			foo('D');
		}
		JamesUtil.printDivider();

		try {
			return;
		} finally {
			System.out.println("Finally");
		}
	}
}
