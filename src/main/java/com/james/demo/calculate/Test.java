package com.james.demo.calculate;

public class Test {
    /**
     * Compute batch count according to total thread count and batch size
     * 
     * @param total
     * @param batchCount
     * @return
     */
    public static int getBatchSize(int total, int batchCount) {
        int a = total / batchCount;
        int b = total % batchCount;

        if (0 == b) {
            return a;
        } else {
            return a + 1;
        }
    }

    public static void main(String[] args) {
        int threadCount = 5;
        int total = 52;

        System.out.println(getBatchSize(total, threadCount));
    }
}
