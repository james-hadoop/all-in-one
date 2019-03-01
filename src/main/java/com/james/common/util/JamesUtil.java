package com.james.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.james.dataStructure.tree.BinTree.TreeNode;

public class JamesUtil {
	public static void printDivider() {
		System.out.println("\n---------------------------------- divider ----------------------------------\n\n");
	}

	public static void printSet(Set<String> set) {
		if (null == set || 0 == set.size()) {
			return;
		}

		for (String s : set) {
			System.out.println(s);
		}
	}

	public static void printStack(Stack<String> stack) {
		if (null == stack || 0 == stack.size()) {
			return;
		}

		for (String s : stack) {
			System.out.println(s);
		}
	}

	public static void printArray(int[] arr) {
		for (int i = 0; i < arr.length; i++) {
			System.out.print(arr[i]);
			System.out.print("\t");
		}
		System.out.println();
	}

	public static void printArray(String[] arr) {
		for (int i = 0; i < arr.length; i++) {
			System.out.print(arr[i]);
			System.out.print("\t");
		}
		System.out.println();
	}

	public static void printList(List<String> list) {
		if (null == list || 0 == list.size()) {
			return;
		}

		for (int i = 0; i < list.size(); i++) {
			System.out.print(list.get(i));
			System.out.print("\t");
		}
		System.out.println();
	}

	public static void printTreeList(List<TreeNode> list, String mode) {
		System.out.println(mode + " tree: ");
		for (TreeNode node : list) {
			System.out.print(node.data + "\t");
		}
		System.out.println("\n");
	}

	public static void printMap(Map<String, Object> map) {
		if (null == map || 0 == map.size()) {
			return;
		}

		Set<String> setKey = map.keySet();
		for (String key : setKey) {
			System.out.println("key: " + key + " --> " + "value: " + map.get(key));
		}
		System.out.println();
	}

	public static void printStringMap(Map<String, String> map) {
		if (null == map || 0 == map.size()) {
			return;
		}

		Set<String> setKey = map.keySet();
		for (String key : setKey) {
			System.out.println(key + " --> " + map.get(key));
		}
		System.out.println();
	}

	public static List<String> string2List(String str, String divider) {
		if (null == str || 0 == str.length() || null == divider || 0 == divider.length()) {
			return null;
		}

		List<String> list = new ArrayList<String>();

		if (!str.contains(divider)) {
			list.add(str);
			return list;
		}

		String[] strArr = str.split(divider);
		list = Arrays.asList(strArr);

		return list;
	}

	public static List<String> removeAs(List<String> list) {
		if (null == list || 0 == list.size()) {
			return null;
		}

		List<String> listClean = new ArrayList<String>();

		for (int i = 0; i < list.size(); i++) {
			listClean.add(removeAsInString(list.get(i)));
		}

		return listClean;
	}

	public static String removeAsInString(String str) {
		if (null == str || 0 == str.length()) {
			return null;
		}

		String text = str;
		if (str.contains("AS")) {
			text = str.substring(0, str.indexOf(" AS "));
		}

		if (text.contains(".")) {
			text = text.substring(text.indexOf(".") + 1, text.length());
		}

		return text;
	}

	public static void main(String[] args) {
		String str = "tt.a_a AS f_a_a";
		String text = removeAsInString(str);
		System.out.println(text);

		String strArr = "tt.a_a AS f_a_a,tt.b_b AS f_b_b";
		List<String> list = removeAs(string2List(strArr, ","));
		JamesUtil.printList(list);
	}
}
