package com.james.dataStructure.sort;

import com.james.common.util.JamesUtil;

public class InsertSort {
    public static void insertSort(int[] a) {
        int j;

        for (int p = 1; p < a.length; p++) {
            int tmp = a[p];

            for (j = p; j > 0 && tmp < a[j - 1]; j--) {
                a[j] = a[j - 1];
            }

            a[j] = tmp;
        }
    }

    public static void main(String[] args) {
        int[] arr = { 34, 8, 64, 51, 32, 21 };
        System.out.println("before sort...");
        JamesUtil.printArray(arr);
        JamesUtil.printDivider();

        System.out.println("after sort...");
        insertSort(arr);
        JamesUtil.printArray(arr);
        JamesUtil.printDivider();
    }
}
