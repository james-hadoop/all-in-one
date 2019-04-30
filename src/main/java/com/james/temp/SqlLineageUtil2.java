package com.james.temp;

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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

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

public class SqlLineageUtil2 {
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

		if (queryField.contains(".")) {
			return queryField;
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
	 * 20190411
	 */
	public static Set<String> addAliasName3(String field, Set<String> joinTopLevelTableSet) {
		if (null == field) {
			return null;
		}

		Set<String> set = new HashSet<String>();

		if (field.contains(".")) {
			set.add(field);
		} else {
			for (String k : joinTopLevelTableSet) {
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
	 * 20190411
	 */
	public static Set<String> addReferTableName3(String field, Map<String, ArrayList<String>> tableNameAliasMap) {
		if (null == field || !field.contains(".")) {
			return null;
		}

		// TableLineageInfo info = tableNameAliasMap.get(field.substring(0,
		// field.indexOf(".")));
		// if (null == info) {
		// return null;
		// }

		Set<String> set = new HashSet<String>();
		//
		// if (info.getTableAliasReferMap().size() > 1) {
		// for (String k : info.getTableAliasReferMap().keySet()) {
		// set.add(k + "." + field.substring(field.indexOf(".") + 1));
		// }
		// } else {
		// set.add(field);
		// }

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

	public static String getTableName(String fieldName) {
		if (null == fieldName || 0 == fieldName.length() || !fieldName.contains(".")) {
			return null;
		}

		return fieldName.substring(0, fieldName.indexOf("."));
	}

	public static String getFieldName(String fieldName) {
		if (null == fieldName || 0 == fieldName.length() || !fieldName.contains(".")) {
			return null;
		}

		return fieldName.substring(fieldName.indexOf(".") + 1, fieldName.length());
	}

	public static String replaceTableName(String fieldName, String tgtTableName) {
		if (null == fieldName || null == tgtTableName || !fieldName.contains(".")) {
			return fieldName;
		}

		return tgtTableName + "." + getFieldName(fieldName);
	}

	/**
	 * flattenedTopLevelTableAliasMap: {C=[A, B], c=[a, b], D=[M, N], T=[M, N],
	 * F=[M, N], G=[M, N], X=[M, N], Y=[M, N], Z=[a, b]}
	 * 
	 * Z.data => [a.data, b.data]
	 * 
	 * @param fieldName
	 * @param flattenedTopLevelTableAliasMap
	 * @return
	 */
	public static Set<String> transformTo1stLevelAliasTableName(String fieldName,
			Map<String, HashSet<String>> flattenedTopLevelTableAliasMap) {
		if (null == fieldName || 0 == fieldName.length() || !fieldName.contains(".")
				|| null == flattenedTopLevelTableAliasMap || 0 == flattenedTopLevelTableAliasMap.size()) {
			return null;
		}

		HashSet<String> tableAliasSet = flattenedTopLevelTableAliasMap.get(getTableName(fieldName));

		Set<String> retSet = new HashSet<String>();

		if (null != tableAliasSet) {
			for (String a : tableAliasSet) {
				retSet.add(a + "." + getFieldName(fieldName));
			}
		}
		retSet.add(fieldName);

		return retSet;
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

	public static Map<String, String> flattenTableNameAliasMap(Set<String> flateningTableNameAliasSet,
			Map<String, ArrayList<String>> tableNameAliasMap, List<ImmutablePair<String, String>> flattenedPairList,
			String cursor) {
		if (null == flateningTableNameAliasSet || 0 == flateningTableNameAliasSet.size() || null == tableNameAliasMap
				|| 0 == tableNameAliasMap.size()) {
			return null;
		}

		Map<String, String> map = new HashMap<String, String>();

		for (String s : flateningTableNameAliasSet) {
			List<String> mappingTableNameList = tableNameAliasMap.get(s);

			String cursorTable = null != cursor ? cursor : s;
			if (null != mappingTableNameList) {
				for (String mappingTableName : mappingTableNameList) {
					map.put(s, mappingTableName);

					flattenedPairList.add(new ImmutablePair<String, String>(cursorTable, mappingTableName));

					if (null != tableNameAliasMap.get(mappingTableName)) {
						Set<String> tempSet = new HashSet<String>();
						tempSet.add(mappingTableName);
						flattenTableNameAliasMap(tempSet, tableNameAliasMap, flattenedPairList, cursorTable);
					}
				}
			} else {
				flattenedPairList.add(new ImmutablePair<String, String>(s, s));
				return null;
			}
		}
		return null;

	}

	public static Map<String, ArrayList<String>> fillAliasTableName(Map<String, String> insertSelectFieldMap,
			Map<String, String> fieldAliasMap, Set<String> topLevelTableSet) {
		if (null == insertSelectFieldMap || null == topLevelTableSet || null == fieldAliasMap) {
			return null;
		}

		Map<String, ArrayList<String>> finalFieldMap = new HashMap<String, ArrayList<String>>();

		for (Entry<String, String> entry : insertSelectFieldMap.entrySet()) {
			String finalFieldName = entry.getKey();
			String fieldAliasName = SqlLineageUtil2.findSrcField(entry.getValue(), fieldAliasMap);

			ArrayList<String> fieldAliasList = new ArrayList<String>();
			if (!fieldAliasName.contains(".")) {
				for (String alias : topLevelTableSet) {
					fieldAliasList.add(alias + "." + fieldAliasName);
				}
			} else {
				fieldAliasList.add(fieldAliasName);
			}

			finalFieldMap.put(finalFieldName, fieldAliasList);
		}

		return finalFieldMap;
	}

	public static Map<String, String> transformToReferTableName(Map<String, TableLevelNode> tableLevelNodeMap,
			Map<String, String> tableAliasMap) {
		if (null == tableLevelNodeMap || null == tableAliasMap) {
			return null;
		}

		Map<String, String> referTableNameMap = new HashMap<String, String>();

		for (Entry<String, TableLevelNode> e : tableLevelNodeMap.entrySet()) {
			if (2 > e.getValue().getLevel()) {
				continue;
			}

			referTableNameMap.put(e.getKey(), e.getValue().getParent().getTableName());
		}

		return referTableNameMap;
	}

	public static Map<String, ArrayList<String>> transformToInsideLevelTableName(
			Map<String, ArrayList<String>> finalFieldMap, Map<String, TableLevelNode> tableLevelNodeMap) {
		if (null == tableLevelNodeMap || null == finalFieldMap) {
			return null;
		}

		Map<String, ArrayList<String>> insideLevelFieldMap = new HashMap<String, ArrayList<String>>();
		for (Entry<String, ArrayList<String>> e : finalFieldMap.entrySet()) {
			ArrayList<String> fieldAliasList = e.getValue();

			ArrayList<String> insideLevelTableNameList = new ArrayList<String>();
			for (String fieldAlias : fieldAliasList) {

				if (null != tableLevelNodeMap.get(getTableName(fieldAlias))
						&& 1 == tableLevelNodeMap.get(getTableName(fieldAlias)).getLevel()) {
					String insideLevelField = fieldAlias;
					insideLevelTableNameList.add(insideLevelField);
					continue;
				}

				for (Entry<String, TableLevelNode> tableLevelNode : tableLevelNodeMap.entrySet()) {
					if (1 > tableLevelNode.getValue().getLevel()
							|| !tableLevelNode.getValue().getParent().getTableName().equals(getTableName(fieldAlias))) {
						break;
					}

					String insideLevelField = replaceTableName(fieldAlias, tableLevelNode.getKey());
					insideLevelTableNameList.add(insideLevelField);
				}

			}

			insideLevelFieldMap.put(e.getKey(), insideLevelTableNameList);
		}

		return insideLevelFieldMap;
	}

	private static boolean isNeedReplace(String tableAlias, Map<String, String> referTableNameMap) {
		if (null == tableAlias || null == referTableNameMap) {
			return false;
		}

		for (Entry<String, String> entry : referTableNameMap.entrySet()) {
			if (entry.getValue().equals(tableAlias)) {
				return true;
			}
		}

		return false;
	}

	public static Map<String, ArrayList<String>> transformToInsideLevelTableName2(
			Map<String, ArrayList<String>> finalFieldMap, Map<String, String> referTableNameMap,
			Map<String, TableLevelNode> tableLevelNodeMap) {
		if (null == finalFieldMap || null == referTableNameMap || null == tableLevelNodeMap) {
			return null;
		}

		Map<String, ArrayList<String>> insideLevelFieldMap = new HashMap<String, ArrayList<String>>();
		for (Entry<String, ArrayList<String>> e : finalFieldMap.entrySet()) {
			ArrayList<String> fieldAliasList = e.getValue();

			ArrayList<String> insideLevelTableNameList = new ArrayList<String>();
			for (String fieldAlias : fieldAliasList) {
				if (false == isNeedReplace(getTableName(fieldAlias), referTableNameMap)) {
					String insideLevelField = fieldAlias;
					insideLevelTableNameList.add(insideLevelField);
				} else {

					for (Entry<String, String> entry : referTableNameMap.entrySet()) {
						if (getTableName(fieldAlias).equals(entry.getValue())) {
							String insideLevelField = replaceTableName(fieldAlias, entry.getKey());
							insideLevelTableNameList.add(insideLevelField);
						}
					}
				}
			}

			ArrayList<String> insideLevelTableNameListNew = new ArrayList<String>();
			insideLevelTableNameListNew.addAll(insideLevelTableNameList);
			insideLevelFieldMap.put(e.getKey(), insideLevelTableNameListNew);
			insideLevelTableNameList.clear();
		}

		return insideLevelFieldMap;
	}

	public static Map<String, String> calculateFinalFieldMapping(Map<String, ArrayList<String>> insideLevelFieldMap,
			Map<String, String> tableAliasMap, Set<String> fieldAliasSet) {
		if (null == insideLevelFieldMap || null == tableAliasMap || null == fieldAliasSet) {
			return null;
		}

		Map<String, String> finalFieldMapping = new HashMap<String, String>();

		for (Entry<String, ArrayList<String>> entry : insideLevelFieldMap.entrySet()) {
			ArrayList<String> insideLevelFieldList = entry.getValue();
			for (String s : insideLevelFieldList) {
				for (Entry<String, String> tableAliasEntry : tableAliasMap.entrySet()) {
					if (fieldAliasSet.contains(s) && getTableName(s).equals(tableAliasEntry.getKey())) {
						String finalFieldName = replaceTableName(s, tableAliasEntry.getValue());
						finalFieldMapping.put(entry.getKey(), finalFieldName);
						break;
					}
				}
			}
		}

		return finalFieldMapping;
	}
}
