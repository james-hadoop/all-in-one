package com.james.common.util;

import java.util.List;

import com.james.dataStructure.tree.BinTree.TreeNode;

public class JamesUtil {
    public static void printDivider() {
        System.out.println("\n----------------------------------------------------\n\n");
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
}
