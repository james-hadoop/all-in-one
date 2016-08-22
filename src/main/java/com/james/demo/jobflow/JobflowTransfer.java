package com.james.demo.jobflow;

import java.util.ArrayList;
import java.util.List;

import com.james.demo.jobflow.GlobalCnstant.GlobalConstant;

public class JobflowTransfer {
    private static int step = 0;

    private static void sortJobFlowDetailOrder(List<TaskJobFlowDetailInfo> taskJobFlowDetailInfoList,
            List<Integer> parentIdList) {
        if (taskJobFlowDetailInfoList == null || taskJobFlowDetailInfoList.size() == 0) {
            return;
        }

        step++;

        int stepItemOrder = 0;
        List<TaskJobFlowDetailInfo> leftList = new ArrayList<>();

        List<Integer> nextStepParentIdList = new ArrayList<>();
        for (TaskJobFlowDetailInfo item : taskJobFlowDetailInfoList) {
            String[] parentIdParts = item.getParentJobId().split(GlobalConstant.DIVIDER_BETWEEN_ITEMS);
            boolean isInParent = true;
            for (String parentId : parentIdParts) {
                isInParent = isInParent & parentIdList.contains(Integer.valueOf(parentId.trim()));
            }
            if (isInParent) {
                nextStepParentIdList.add(item.getJobId());
                stepItemOrder++;
                item.setStep(step);
                item.setStepItemOrder(stepItemOrder);
            } else {
                leftList.add(item);
            }
        }

        parentIdList.addAll(nextStepParentIdList);

        if (leftList.size() > 0) {
            sortJobFlowDetailOrder(leftList, parentIdList);
        }
    }

    public static void main(String[] args) {
        TaskJobFlowDetailInfo j1 = new TaskJobFlowDetailInfo();
        j1.setJobId(1);
        j1.setParentJobId("0");

        TaskJobFlowDetailInfo j2 = new TaskJobFlowDetailInfo();
        j2.setJobId(2);
        j2.setParentJobId("0");

        TaskJobFlowDetailInfo j3 = new TaskJobFlowDetailInfo();
        j3.setJobId(3);
        j3.setParentJobId("1");

        TaskJobFlowDetailInfo j4 = new TaskJobFlowDetailInfo();
        j4.setJobId(4);
        j4.setParentJobId("1");

        TaskJobFlowDetailInfo j5 = new TaskJobFlowDetailInfo();
        j5.setJobId(5);
        j5.setParentJobId("2");

        TaskJobFlowDetailInfo j6 = new TaskJobFlowDetailInfo();
        j6.setJobId(6);
        j6.setParentJobId("2");

        TaskJobFlowDetailInfo j7 = new TaskJobFlowDetailInfo();
        j7.setJobId(7);
        j7.setParentJobId("3|4|5");

        TaskJobFlowDetailInfo j8 = new TaskJobFlowDetailInfo();
        j8.setJobId(8);
        j8.setParentJobId("6");

        TaskJobFlowDetailInfo j9 = new TaskJobFlowDetailInfo();
        j9.setJobId(9);
        j9.setParentJobId("1|6");

        List<Integer> parentIdList = new ArrayList<Integer>();
        parentIdList.add(0);

        List<TaskJobFlowDetailInfo> listJ = new ArrayList<TaskJobFlowDetailInfo>();
        listJ.add(j1);
        listJ.add(j2);
        listJ.add(j3);
        listJ.add(j4);
        listJ.add(j5);
        listJ.add(j6);
        listJ.add(j7);
        listJ.add(j8);
        listJ.add(j9);

        System.out.println("before...");
        for (TaskJobFlowDetailInfo j : listJ) {
            System.out.println(j.getJobId() + "\t" + j.getParentJobId() + "\t" + j.getStep() + "\t"
                    + j.getStepItemOrder());
        }

        sortJobFlowDetailOrder(listJ, parentIdList);

        System.out.println("after...");
        for (TaskJobFlowDetailInfo j : listJ) {
            System.out.println(j.getJobId() + "\t" + j.getParentJobId() + "\t" + j.getStep() + "\t"
                    + j.getStepItemOrder());
        }
    }
}
