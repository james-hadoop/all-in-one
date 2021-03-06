package com.james.common.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

import com.james.hive.parser.entity.TableLineageInfo;
import com.james.hive.parser.entity.TableNode;
import com.james.hive.parser.entity.TableRelation;

import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.viz.ColorImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.viz.PositionImpl;

public class SqlLineageUtil {
	/**
	 * fieldAliasMap中递归查找原始字段的名称
	 * 
	 * @param queryField
	 * @param fieldAliasMap
	 * @return
	 */
	public static String findSrcField(String queryField, Map<String, String> fieldAliasMap) {
		if (null == queryField || null == fieldAliasMap || 0 == fieldAliasMap.size()) {
			return null;
		}

		String srcField = fieldAliasMap.get(queryField);
		if (null == srcField || queryField.equals(srcField)) {
			return queryField;

		} else {
			return findSrcField(srcField, fieldAliasMap);
		}
	}

	/**
	 * tableAliasLineageMap中查找原始表的名称
	 * 
	 * @param tableAliasName
	 * @param tableAliasLineageMap
	 * @return
	 */
	public static Set<String> findSrcTable(String tableAliasName, Map<String, TableLineageInfo> tableAliasLineageMap) {
		if (null == tableAliasName || null == tableAliasLineageMap || 0 == tableAliasLineageMap.size()) {
			return null;
		}

		return tableAliasLineageMap.get(tableAliasName).getTableAliasReferMap().keySet();
	}

	public static String replaceWithSrcTableName(String aliasFieldName,
			Map<String, TableLineageInfo> tableAliasLineageMap) {
		if (null == aliasFieldName || !aliasFieldName.contains(".") || null == tableAliasLineageMap
				|| 0 == tableAliasLineageMap.size()) {
			return null;
		}

		String aliasTableName = aliasFieldName.substring(0, aliasFieldName.indexOf("."));

		String srcFieldName = aliasFieldName.replace(aliasTableName + ".",
				tableAliasLineageMap.get(aliasTableName).getTableAliasReferMap().get(aliasTableName) + ".");

		return srcFieldName;
	}

	public static Set<String> findSrcTable(String tableAliasName, Map<String, TableLineageInfo> tableAliasLineageMap,
			Stack<String> tokTableNameStack) {
		if (null == tableAliasName || null == tableAliasLineageMap || 0 == tableAliasLineageMap.size()) {
			return null;
		}

		return tableAliasLineageMap.get(tableAliasName).getTableAliasReferMap().keySet();
	}

	/*
	 * vv -> c.vv
	 */
	public static Set<String> addAliasName(String field, Map<String, String> topLevelTableAliasMap) {
		if (null == field) {
			return null;
		}

		Set<String> set = new HashSet<String>();

		if (field.contains(".")) {
			set.add(field);
		} else {
			Set<String> s = topLevelTableAliasMap.keySet();
			for (String k : s) {
				set.add(k + "." + field);
			}
		}

		return set;
	}

	/*
	 * c.puin -> a.puin & b.puin
	 */
	public static Set<String> addReferTableName(String field, Map<String, TableLineageInfo> tableAliasLineageMap) {
		if (null == field || !field.contains(".")) {
			return null;
		}

		TableLineageInfo info = tableAliasLineageMap.get(field.substring(0, field.indexOf(".")));
		if (null == info) {
			return null;
		}

		Set<String> set = new HashSet<String>();

		if (info.getTableAliasReferMap().size() > 1) {
			for (String k : info.getTableAliasReferMap().keySet()) {
				set.add(k + "." + field.substring(field.indexOf(".") + 1));
			}
		} else {
			set.add(field);
		}

		return set;
	}

	/*
	 * b.history_vv -> op_cnt
	 * 
	 * b.history_vv -> b.op_cnt
	 */
	public static Set<String> addAliasName2(String field, Map<String, String> fieldAliasMap2) {
		if (null == field || !field.contains(".")) {
			return null;
		}

		String mappingField = fieldAliasMap2.get(field);
		if (null == mappingField) {
			return null;
		}

		Set<String> set = new HashSet<String>();
		String aliasName = field.substring(0, field.indexOf("."));

		if (mappingField.contains(".")) {
			set.add(mappingField);
		} else {
			set.add(aliasName + "." + mappingField);
		}

		return set;
	}

	public static TableRelation generateTableRelation(Map<String, String> fieldMap, String tgtTableName) {
		if (null == fieldMap || 0 == fieldMap.size() || null == tgtTableName) {
			return null;
		}

		// target
		List<String> tgtFieldList = new ArrayList<String>();
		tgtFieldList.addAll(fieldMap.keySet());

		TableNode target = new TableNode(tgtTableName, tgtFieldList);

		// sources
		Map<String, List<String>> srcTableFieldMap = parseSrcTableField(fieldMap);
		List<TableNode> sources = new ArrayList<TableNode>();

		for (Entry<String, List<String>> e : srcTableFieldMap.entrySet()) {
			sources.add(new TableNode(e.getKey(), e.getValue()));
		}

		TableRelation tableRelation = new TableRelation(sources, target, fieldMap);

		return tableRelation;
	}

	public static String makeGexf(TableRelation relation) throws IOException {
		if (null == relation) {
			return null;
		}

		Gexf gexf = new GexfImpl();
		Calendar date = Calendar.getInstance();

		gexf.getMetadata().setLastModified(date.getTime()).setCreator("james").setDescription("TableRelation");
		gexf.setVisualization(true);

		Graph graph = gexf.getGraph();
		graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);

		AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
		graph.getAttributeLists().add(attrList);

		Attribute clazz = attrList.createAttribute("modularity_class", AttributeType.INTEGER, "Class");

		Node tgtNode = graph.createNode("0");
		tgtNode.setLabel(relation.getTarget().getTableName()).getAttributeValues().addValue(clazz, "0");
		tgtNode.setSize(50).setPosition(new PositionImpl(-200, 200, 0)).setColor(new ColorImpl(235, 81, 72));

		List<Node> srcNodeList = new ArrayList<Node>();
		for (int i = 0; i < relation.getSources().size(); i++) {
			Node srcNode = graph.createNode(Integer.toString(i + 1));
			srcNode.setLabel(relation.getSources().get(i).getTableName()).getAttributeValues().addValue(clazz,
					Integer.toString(i + 2));
			srcNode.setSize(50).setPosition(new PositionImpl(-300, 100 * (i + 1), 0))
					.setColor(new ColorImpl(235, 81, 72));

			srcNodeList.add(srcNode);
		}

		for (Node srcNode : srcNodeList) {
			srcNode.connectTo("0", tgtNode).setWeight(5.0f);
		}

		StaxGraphWriter graphWriter = new StaxGraphWriter();
		File f = new File("data/gexf/table_relation.gexf");
		f.createNewFile();

		Writer out;
		try {
			out = new FileWriter(f, false);
			graphWriter.writeToStream(gexf, out, "UTF-8");
			System.out.println(f.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}
	
	public static String unescapeIdentifier(String val) {
		if (val == null) {
			return null;
		}
		if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
			val = val.substring(1, val.length() - 1);
		}
		return val;
	}

	private static Map<String, List<String>> parseSrcTableField(Map<String, String> fieldMap) {
		if (null == fieldMap || 0 == fieldMap.size()) {
			return null;
		}

		Map<String, List<String>> map = new HashMap<String, List<String>>();
		for (String v : fieldMap.values()) {
			String table = v.substring(0, v.lastIndexOf("."));
			String field = v.substring(v.lastIndexOf(".") + 1, v.length());

			if (null == map.get(table)) {
				List<String> list = new ArrayList<String>();
				list.add(field);

				map.put(table, list);
			} else {
				map.get(table).add(field);
			}
		}

		return map;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortMapByValue(Map<K, V> map) {
		Map<K, V> result = new LinkedHashMap<>();
		Stream<Entry<K, V>> st = map.entrySet().stream();

		st.sorted(Comparator.comparing(e -> e.getValue())).forEach(e -> result.put(e.getKey(), e.getValue()));

		return result;
	}
}
