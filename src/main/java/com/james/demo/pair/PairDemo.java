package com.james.demo.pair;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class PairDemo {
	public static void main(String[] args) {
		System.out.println("start...");

//		Pair<String,String> e=new Pair<String,String>("key", "value");
		Pair<String, String> e = new ImmutablePair<String, String>("key", "value");

		System.out.println(e.getKey() + " -> " + e.getValue());
		System.out.println(e.getLeft() + " -> " + e.getRight());

		System.out.println("stop ...");
	}
}
