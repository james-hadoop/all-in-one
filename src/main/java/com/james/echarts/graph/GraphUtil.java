package com.james.echarts.graph;

import java.util.ArrayList;
import java.util.List;

public class GraphUtil {
	public static String generateEchatsOption(int startX, int startY, int spaceX, int spaceY,
			TableRelation tableRelations) {
		if (0 >= spaceX || 0 >= spaceY || 0 >= startX || 0 >= startY) {
			return null;
		}

		if (null == tableRelations || null == tableRelations.getSources() || 0 == tableRelations.getSources().size()
				|| null == tableRelations.getTarget()) {
			return null;
		}

		StringBuilder sbData = new StringBuilder("data:[");
		StringBuilder sbLinks = new StringBuilder("links:[");

		// source tables
		int srcCount = 1;
		for (TableNode src : tableRelations.getSources()) {
			String tableName = src.getTableName();
			int x = startX;
			int y = startY + (srcCount - 1) * spaceY;

			sbData.append("{name:'" + tableName + "',x:" + x + ",y:" + y + "},");

			srcCount++;
		}

		// target tables
		String tgtTableName = tableRelations.getTarget().getTableName();

		int x = startX + spaceX;
		int y = (2 * startY + (tableRelations.getSources().size() - 1) * spaceY) / 2;
		sbData.append("{name:'" + tgtTableName + "',x:" + x + ",y:" + y + "},");

		// relations from sources to target
		for (TableNode src : tableRelations.getSources()) {
			String srcTableName = src.getTableName();

			sbLinks.append("{source:'" + srcTableName + "',target:'" + tgtTableName + "'},");
		}

		String header = "{animationDurationUpdate:1500,animationEasingUpdate:'quinticInOut',series:[{type:'graph',layout:'none',symbolSize:50,roam:true,label:{normal:{position: 'top',show:true}},edgeSymbol:['circle','arrow'],edgeSymbolSize:[4,10],edgeLabel:{normal:{textStyle:{fontSize:20}}}";

		String data = sbData.substring(0, sbData.length() - 1) + "]";
		String links = sbLinks.toString() + "]";
		String tail = "lineStyle:{normal:{opacity:0.9,width:2,curveness:0}}}]}";

		return header + "," + data + "," + links + "," + tail;
	}

	public static void main(String[] args) {
		TableNode src1 = new TableNode("src1");
		TableNode src2 = new TableNode("src2");
		TableNode src3 = new TableNode("src3");
		TableNode tgt = new TableNode("tgt");

		List<TableNode> srcs = new ArrayList<TableNode>();
		srcs.add(src1);
		srcs.add(src2);
//		srcs.add(src3);

		TableRelation relation = new TableRelation(srcs, tgt);

		String option = generateEchatsOption(300, 100, 50, 50, relation);
		System.out.println("option=" + option);
	}
}
