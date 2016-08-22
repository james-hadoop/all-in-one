package com.james.demo.jobflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JobflowDependencyGraph {
    private static final String DIVIDER_BETWEEN_ITEMS = "\\|";

    private Map<Integer, Integer> mapLayerAndNodeCount = new HashMap<Integer, Integer>();

    private Map<Integer, Node> mapNodes = new HashMap<Integer, Node>();

    class Node {
        private int id;
        private String name;
        private List<Integer> dependentIds;
        private int currentLayer;
        private int currentLayerNodeCount;

        public Node(int id, String name, List<Integer> dependentIds) {
            this.id = id;
            this.name = name;
            this.dependentIds = dependentIds;
        }

        public Node() {
            new Node(0, null, null);
        }

        public String toString() {
            return "id=" + id + "    name=" + name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<Integer> getDependentIds() {
            return dependentIds;
        }

        public void setDependentIds(List<Integer> dependentIds) {
            this.dependentIds = dependentIds;
        }

        public int getCurrentLayer() {
            return currentLayer;
        }

        public void setCurrentLayer(int currentLayer) {
            this.currentLayer = currentLayer;
        }

        public int getCurrentLayerNodeCount() {
            return currentLayerNodeCount;
        }

        public void setCurrentLayerNodeCount(int currentLayerNodeCount) {
            this.currentLayerNodeCount = currentLayerNodeCount;
        }
    }

    public List<Integer> string2List(String str, String token) {
        List<Integer> listInt = new ArrayList<Integer>();

        if (null == str || 0 == str.length()) {
            return listInt;
        }

        String[] arrStr = str.split(token);
        for (String s : arrStr) {
            listInt.add(Integer.valueOf(s));
        }

        return listInt;
    }

    public JobflowDependencyGraph transform(List<TaskJobFlowDetailInfo> taskJobFlowDetailInfoList) {
        if (null == taskJobFlowDetailInfoList || 0 == taskJobFlowDetailInfoList.size()) {
            return null;
        }

        for (TaskJobFlowDetailInfo j : taskJobFlowDetailInfoList) {
            Node n = this.new Node(j.getJobId(), j.getJobName(), string2List(j.getParentJobId(), DIVIDER_BETWEEN_ITEMS));
            this.mapNodes.put(n.getId(), n);
        }

        setNodeLayer(this.mapNodes);

        return this;
    }

    public int maxInteger(List<Integer> integers) {
        if (null == integers || 0 == integers.size()) {
            return -1;
        }

        int max = -1;
        for (Integer i : integers) {
            if (i > max) {
                max = i;
            }
        }

        return max;
    }

    public void setNodeLayer(Map<Integer, Node> mapNode) {
        if (null == mapNode || 0 == mapNode.size()) {
            return;
        }

        Set<Integer> keys = mapNode.keySet();

        for (Integer key : keys) {
            Node node = mapNode.get(key);
            List<Integer> dependentNodeLayers = new ArrayList<Integer>();

            List<Integer> dependentNodeIds = node.getDependentIds();

            for (Integer i : dependentNodeIds) {
                if (0 == i) {
                    dependentNodeLayers.add(0);
                } else {
                    dependentNodeLayers.add(mapNode.get(i).getCurrentLayer());
                }
            }

            int nCurrentLayer = 1 + maxInteger(dependentNodeLayers);
            node.setCurrentLayer(nCurrentLayer);

            if (mapLayerAndNodeCount.containsKey(nCurrentLayer)) {
                mapLayerAndNodeCount.put(nCurrentLayer, mapLayerAndNodeCount.get(nCurrentLayer) + 1);
            } else {
                mapLayerAndNodeCount.put(nCurrentLayer, 1);
            }
        }
    }

    public Map<Integer, Integer> getMapLayerAndNodeCount() {
        return mapLayerAndNodeCount;
    }

    public void setMapLayerAndNodeCount(Map<Integer, Integer> mapLayerAndNodeCount) {
        this.mapLayerAndNodeCount = mapLayerAndNodeCount;
    }

    public Map<Integer, Node> getMapNodes() {
        return mapNodes;
    }

    public void setMapNodes(Map<Integer, Node> mapNodes) {
        this.mapNodes = mapNodes;
    }

    public static void main(String[] args) {
        JobflowDependencyGraph graph = new JobflowDependencyGraph();

        Node n1 = graph.new Node(1, "n1", null);
        Node n2 = graph.new Node();

        System.out.println(n1);
        System.out.println(n2);
        System.out.println("\n");

        String str = "1" + "|" + "2" + "|" + "3";
        List<Integer> listInt = graph.string2List(str, DIVIDER_BETWEEN_ITEMS);
        for (Integer i : listInt) {
            System.out.print(i + "\t");
        }
        System.out.println("\n\n");

        //
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
        j9.setParentJobId("2|6");

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

        JobflowDependencyGraph g = graph.transform(listJ);

        Map<Integer, Node> mapNode = g.getMapNodes();

        Set<Integer> keys = mapNode.keySet();

        for (Integer key : keys) {
            Node node = mapNode.get(key);

            System.out.print("  " + key + "(" + node.getCurrentLayer() + ","
                    + g.getMapLayerAndNodeCount().get(node.getCurrentLayer()) + ")");
        }
        System.out.println();
    }
}
