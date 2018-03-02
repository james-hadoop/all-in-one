package com.james.common.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.james.dataStructure.tree.BinTree.TreeNode;

public class JamesUtil {
    public static void printDivider() {
        System.out.println("\n---------------------------------- divider ----------------------------------\n\n");
    }

    public static void printArray(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i]);
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
}
