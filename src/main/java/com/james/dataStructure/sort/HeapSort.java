package com.james.dataStructure.sort;

public class HeapSort {

    public static void main(String[] args) {
        int[] data = { 21, 30, 49, 30, 21, 16, 9 };
        System.out.println("排序之前：\n\t" + java.util.Arrays.toString(data));

        System.out.println();
        heapSort(data);

        System.out.println();
        System.out.println("排序之后：\n\t" + java.util.Arrays.toString(data));
    }

    public static void heapSort(int[] data) {
        System.out.println("开始排序...");

        int arrayLength = data.length;

        for (int i = 0; i < arrayLength - 1; i++) {
            buildMaxdHeap(data, arrayLength - 1 - i);
        }
    }

    private static void buildMaxdHeap(int[] data, int lastIndex) {
        // 从lastIndex处节点（最后一个节点）的父节点开始
        for (int i = (lastIndex - 1) / 2; i >= 0; i--) {
            // k保存当前正在判断的节点
            int k = i;

            // 如果当前k节点的子节点存在
            while (k * 2 + 1 <= lastIndex) {
                // k节点左子节点的索引
                int biggerIndex = 2 * k + 1;
                
                // 如果biggerIndex小于lastIndex，即biggerIndex+1代表k节点的右子节点存在
                if (biggerIndex < lastIndex) {
                    // 如果右子节点的值较大
                    if (data[biggerIndex] < (data[biggerIndex + 1])) {
                        // biggerIndex总数记录较大子节点的索引
                        biggerIndex++;
                    }
                }

                // 如果k节点的值小于其较大节点的值
                if (data[k] < data[biggerIndex]) {
                    // 交换它们
                    swap(data, k, biggerIndex);

                    // 将biggerIndex赋给k，开始while循环的下一次循环
                    // 重新保证k节点的值大于其左、右节点的值
                    k = biggerIndex;
                } else {
                    break;
                }
            }
        }
    }

    private static void swap(int[] data, int i, int j) {
        int tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }
}
