package com.james.dataStructure.sort;

public class QuickSort {

    public static void main(String[] args) {
        int[] data = { 21, 30, 49, 30, 21, 16, 9 };
        System.out.println("排序之前：\n\t" + java.util.Arrays.toString(data));

        System.out.println();
        quickSort(data);

        System.out.println();
        System.out.println("排序之后：\n\t" + java.util.Arrays.toString(data));
    }

    public static void quickSort(int[] data) {
        subSort(data, 0, data.length - 1);
    }

    private static void subSort(int[] data, int start, int end) {
        if (start < end) {
            // 以第一个元素作为分界值
            int base = data[start];
            // i从左边开始搜索，搜索大于分界值的元素的索引
            int i = start;
            // j从右边开始搜索，搜索小于分界值的元素的索引
            int j = end + 1;

            while (true) {
                // 找到大于分界值的元素的索引，或者i已经到了end处
                while (i < end && data[++i] <= base)
                    ;
                // 找到小于分界值的元素的索引，或者j已经到了start处
                while (j > start && data[--j] >= base)
                    ;

                if (i < j) {
                    swap(data, i, j);
                } else {
                    break;
                }
            }

            swap(data, start, j);

            // 递归左子序列
            subSort(data, start, j - 1);

            // 递归右子序列
            subSort(data, j + 1, end);
        }
    }

    private static void swap(int[] data, int i, int j) {
        int tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }
}
