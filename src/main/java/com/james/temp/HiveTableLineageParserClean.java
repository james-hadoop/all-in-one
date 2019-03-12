package com.james.temp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import com.james.common.util.JamesUtil;
import com.james.common.util.SqlLineageUtil;
import com.james.hive.parser.entity.TableRelation;

public class HiveTableLineageParserClean {
	/*
	 * TableRelation
	 */
	private List<TableNode> srcTables = new ArrayList<TableNode>();
	private TableNode tgtTable = new TableNode();

	/*
	 * alias tables and fields
	 */
	private Map<String, String> tableAliasMap = new HashMap<String, String>();
	private Map<String, String> insertSelectFieldMap = new TreeMap<String, String>();

	/*
	 * 2 stacks for generating alias table map
	 */
	private Stack<String> tokTableNameStack = new Stack<String>();
	private Stack<String> tokDbNameStack = new Stack<String>();

	/*
	 * TableAliasEntity
	 */
	private Map<String, TableLineageInfo> tableAliasLineageMap = new HashMap<String, TableLineageInfo>();
	private Map<String, String> tableReferAliasMap = new HashMap<String, String>();

	private String currentTable = "";
	private Map<String, String> fieldAliasMap = new TreeMap<String, String>();

	private List<String> aliasFieldList = new ArrayList<String>();
	private List<String> cleanFieldList = new ArrayList<String>();

	private Map<String, String> topLevelTableAliasMap = new HashMap<String, String>();

	// 1st round transform
	private Map<String, String> map1st = new HashMap<String, String>();

	// ParseDriver pd
	private static ParseDriver pd = new ParseDriver();

	// 1st round transform
	private List<ImmutablePair<String, String>> pairList = new ArrayList<ImmutablePair<String, String>>();

	// parseCurrentNode
	private void parseCurrentNode(ASTNode ast) {
		if (ast.getToken() != null) {
			switch (ast.getToken().getType()) {
			case HiveParser.TOK_FROM:
				String tokDbName = "";
				String tokTableName = "";
				if (ast.getChild(0).getChild(0).getChildCount() == 1) {
					// 不带库名
					tokTableName = ast.getChild(0).getChild(0).getChild(0).getText().toLowerCase();
				} else if (ast.getChild(0).getChild(0).getChildCount() == 2) {
					// 带库名
					tokDbName = ast.getChild(0).getChild(0).getChild(0).getText().toLowerCase();
					tokTableName = tokDbName + "." + ast.getChild(0).getChild(0).getChild(1).getText().toLowerCase();
				}

				tokDbNameStack.push(tokDbName.toLowerCase());
				tokTableNameStack.push(tokTableName.toLowerCase());

				break;

			case HiveParser.TOK_SUBQUERY:
				if (ast.getChildCount() == 2) {
					String tableAlias = SqlLineageUtil.unescapeIdentifier(ast.getChild(1).getText()).toLowerCase();
					currentTable = tableAlias;

					tableAliasMap.put(tableAlias.toLowerCase(), tableAlias);

					if (tokDbNameStack.size() > 0) {
						String tokDBAliasName = tokDbNameStack.peek();
						String tokTableAliasName = tokTableNameStack.peek();

						if (tokDBAliasName.equals("tok_query")) {
							tokDbNameStack.pop();
							tokTableNameStack.pop();

							Map<String, String> aliasMap = new HashMap<String, String>();

							Set<String> tableAliasSet = new HashSet<String>();
							do {
								tokDbNameStack.pop();
								String referTableName = tokTableNameStack.pop();

								tableAliasSet.add(referTableName);

								aliasMap.put(tableReferAliasMap.get(referTableName), referTableName);
							} while (tokDbNameStack.size() > 0 && !tokDbNameStack.peek().equals("tok_query"));

							TableLineageInfo tableAliasEntity = new TableLineageInfo(tableAlias, aliasMap);
							tableAliasLineageMap.put(tableAlias, tableAliasEntity);

							Set<String> keyToRemoveSet = new HashSet<String>();
							for (String k : topLevelTableAliasMap.keySet()) {
								if (!topLevelTableAliasMap.get(k).equals("tok_query")) {
									keyToRemoveSet.add(k);
								}
							}
							for (String k : keyToRemoveSet) {
								topLevelTableAliasMap.remove(k);
							}

							topLevelTableAliasMap.put(tableAlias, tokDBAliasName);
						} else {
							String strToAdd = tokTableAliasName;

							if (null == tableAliasLineageMap.get(tableAlias)) {
								Map<String, String> aliasMap = new HashMap<String, String>();
								aliasMap.put(tableAlias, strToAdd);
								TableLineageInfo tableAliasEntity = new TableLineageInfo(tableAlias, aliasMap);
								tableAliasLineageMap.put(tableAlias, tableAliasEntity);
								tableReferAliasMap.put(strToAdd, tableAlias);
							}else {
								tableAliasLineageMap.get(tableAlias).getTableAliasReferMap().put(tableAlias, strToAdd);
								tableReferAliasMap.put(strToAdd, tableAlias);
							}

							topLevelTableAliasMap.put(tableAlias, tokDBAliasName);
						}
					} else {
						// if (tokDbNameStack.size()<=0)
//						System.out.println("tokDbNameStack.size()>0");
					}
				} else {
//					System.out.println("ast.getChildCount() ！= 2");
				}

				for (int i = 0; i < aliasFieldList.size(); i++) {
					fieldAliasMap.put(
							SqlLineageUtil.unescapeIdentifier(ast.getChild(1).getText()).toLowerCase() + "." + aliasFieldList.get(i),
							cleanFieldList.get(i));
				}
				aliasFieldList.clear();
				cleanFieldList.clear();
//				System.out.println("TOK_SUBQUERY");

				break;

			case HiveParser.TOK_TABREF:// inputTable
				ASTNode tabTree = (ASTNode) ast.getChild(0);
				String tableName = (tabTree.getChildCount() == 1)
						? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
						: BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "."
								+ tabTree.getChild(1);
				srcTables.add(new TableNode(tableName));
				if (ast.getChild(1) != null) {
					String alia = ast.getChild(1).getText().toLowerCase();
					tableAliasMap.put(alia.toLowerCase(), tableName);
				}
				break;
			case HiveParser.TOK_SELEXPR:
				String fieldName = "";
				String cleanFieldName = "";
				String aliasFieldName = null == ast.getChild(1) ? null : ast.getChild(1).getText().toLowerCase();

				if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
					fieldName = ast.getChild(0).getChild(0).getText().toLowerCase();
					cleanFieldName = fieldName;
					aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

					System.out.print("currentTable:" + currentTable + "\t");
					System.out.println("字段別名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> " + fieldName);
				} else if (ast.getChild(0).getType() == HiveParser.TOK_FUNCTION) {
					if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_TABLE_OR_COL) {
						fieldName = ast.getChild(0).getChild(0).getText() + "("
								+ ast.getChild(0).getChild(1).getChild(0).getText() + ")";
						cleanFieldName = ast.getChild(0).getChild(1).getChild(0).getText();
						aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

						System.out.print("currentTable:" + currentTable + "\t");
						System.out.println(
								"字段別名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> " + cleanFieldName);

					} else if (ast.getChild(0).getChild(1).getType() == HiveParser.DOT) {
						String tgtTableName = ast.getChild(0).getChild(1).getChild(0).getChild(0).getText()
								.toLowerCase();
						cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getText();
						aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

						System.out.print("currentTable:" + currentTable + "\t");
						System.out.println("字段别名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> "
								+ tgtTableName + "." + cleanFieldName);

					} else if (ast.getChild(0).getChild(0).getType() == HiveParser.KW_WHEN) {
						// System.out.println("HiveParser.KW_WHEN");
						if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_FUNCTION) {
							if (ast.getChild(0).getChild(1).getChild(1).getChildCount() > 1) {
								cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getChild(1).getChild(1)
										.getChild(0).getText().toLowerCase();
								aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;
							} else {
								// (tok_selexpr (tok_function when (tok_function in (tok_table_or_col source)
								// '1' '3') 1 0) is_kd_source)
								cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getChild(0).getText()
										.toLowerCase();
								aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;
							}
						} else if (ast.getChild(0).getChild(1).getType() == HiveParser.EQUAL) {
							// (tok_selexpr (tok_function when (= (tok_table_or_col source) 'hello') 1 0)
							// s_kd_source)
							cleanFieldName = ast.getChild(0).getChild(1).getChild(0).getChild(0).getText()
									.toLowerCase();
							aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;
						} else if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_WINDOWSPEC) {
							// TODO HiveParser.TOK_WINDOWSPEC
						} else {
							// TODO unprocessed situation
						}
					} else {
						// TODO unprocessed situation
					}
				} else if (ast.getChild(0).getType() == HiveParser.DOT) {
					if (ast.getChild(0).getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
						String tgtTableName = ast.getChild(0).getChild(0).getChild(0).getText().toLowerCase();
						cleanFieldName = ast.getChild(0).getChild(1).getText().toLowerCase();
						aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

						System.out.print("currentTable:" + currentTable + "\t");
						System.out.println("字段别名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> "
								+ tgtTableName + "." + cleanFieldName);

					}
				} else if (ast.getChild(0).getType() == HiveParser.TOK_FUNCTIONDI) {
					cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getText().toLowerCase();
					aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

					System.out.print("currentTable:" + currentTable + "\t");
					System.out.println(
							"字段別名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> " + cleanFieldName);
				} else {
					// TODO unprocessed situation
				}

				aliasFieldList.add(aliasFieldName);
				cleanFieldList.add(cleanFieldName);

				// System.out.println("TOK_SELEXPR");
				break;

			case HiveParser.TOK_INSERT_INTO:
				tgtTable.setTableName(ast.getChild(0).getChild(0).getChild(0).getText());

				ASTNode astNode = (ASTNode) ast.getParent().getChild(1);
				int nodeCount = ast.getParent().getChild(1).getChildCount();

				for (int i = 0; i < nodeCount; i++) {
					if (astNode.getChild(i).getChildCount() == 1
							&& astNode.getChild(i).getChild(0).getChildCount() == 0) {
						// (tok_selexpr 20190226) do nothing
					} else if (astNode.getChild(i).getChildCount() == 2
							&& astNode.getChild(i).getChild(0).getChildCount() == 0) {
						// (tok_selexpr 'aaaaa' s_a)
						String fieldCleanName = astNode.getChild(1).getChild(1).getText().toLowerCase();
						String filedAliasName = fieldCleanName;
						insertSelectFieldMap.put(filedAliasName, fieldCleanName);
					} else if (astNode.getChild(i).getChild(0).getType() == HiveParser.DOT) {
						// (. (tok_table_or_col c) row_key))
						String fieldCleanName = astNode.getChild(i).getChild(0).getChild(1).getText().toLowerCase();
						String fieldFromTableName = astNode.getChild(i).getChild(0).getChild(0).getChild(0).getText()
								.toLowerCase();
						String filedAliasName = fieldCleanName;
						if (null != astNode.getChild(i).getChild(1)) {
							filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
						}
						insertSelectFieldMap.put(filedAliasName, fieldFromTableName + "." + fieldCleanName);
					} else if (astNode.getChild(i).getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
						// (tok_selexpr (tok_table_or_col vv) a_vv)
						String fieldCleanName = astNode.getChild(i).getChild(0).getChild(0).getText().toLowerCase();
						String filedAliasName = fieldCleanName;
						if (null != astNode.getChild(i).getChild(1)) {
							filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
						}
						insertSelectFieldMap.put(filedAliasName, fieldCleanName);
					} else if (astNode.getChild(i).getChild(0).getType() == HiveParser.TOK_FUNCTION) {
						// (tok_function when (tok_function in (tok_table_or_col source) '1' '3') 1 0)
						// is_kd_source)
						if (astNode.getChild(i).getChild(0).getChild(0).getType() == HiveParser.KW_WHEN) {
							if (astNode.getChild(i).getChild(0).getChild(1).getType() == HiveParser.TOK_FUNCTION) {
								if (ast.getChild(0).getChild(1).getChild(1).getChildCount() > 1) {
									String fieldCleanName = ast.getChild(0).getChild(1).getChild(1).getChild(1)
											.getChild(1).getChild(0).getText().toLowerCase();
									String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
									insertSelectFieldMap.put(filedAliasName, fieldCleanName);
								}
							} else {
								String fieldCleanName = astNode.getChild(i).getChild(0).getChild(1).getChild(1)
										.getChild(0).getText().toLowerCase();
								String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
								insertSelectFieldMap.put(filedAliasName, fieldCleanName);
							}
							// (tok_selexpr (tok_function when (tok_function in (tok_table_or_col source)
							// '1' '3') 1 0) is_kd_source)
						} else if (astNode.getChild(i).getChild(0).getChild(1).getType() == HiveParser.EQUAL) {
							// (tok_selexpr (tok_function when (= (tok_table_or_col source) 'hello') 1 0)
							// s_kd_source)
							String fieldCleanName = astNode.getChild(i).getChild(0).getChild(1).getChild(0).getChild(0)
									.getText().toLowerCase();
							String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
							insertSelectFieldMap.put(filedAliasName, fieldCleanName);
						}
					}
				}
			} // for
		}
	}

	private void parseIteral(ASTNode ast) {
		parseChildNodes(ast);
		parseCurrentNode(ast);
	}

	private void parseChildNodes(ASTNode ast) {
		if (null == ast) {
			return;
		}

		int childCount = ast.getChildCount();
		if (childCount > 0) {
			for (int i = 0; i < childCount; i++) {
				ASTNode child = (ASTNode) ast.getChild(i);
				parseIteral(child);
			}
		}
	}
	
	public List<TableNode> getSrcTables() {
		return srcTables;
	}

	public void setSrcTables(List<TableNode> srcTables) {
		this.srcTables = srcTables;
	}

	public TableNode getTgtTable() {
		return tgtTable;
	}

	public void setTgtTable(TableNode tgtTable) {
		this.tgtTable = tgtTable;
	}

	public Map<String, String> getTableAliasMap() {
		return tableAliasMap;
	}

	public void setTableAliasMap(Map<String, String> tableAliasMap) {
		this.tableAliasMap = tableAliasMap;
	}

	public Map<String, String> getInsertSelectFieldMap() {
		return insertSelectFieldMap;
	}

	public void setInsertSelectFieldMap(Map<String, String> insertSelectFieldMap) {
		this.insertSelectFieldMap = insertSelectFieldMap;
	}

	public Stack<String> getTokTableNameStack() {
		return tokTableNameStack;
	}

	public void setTokTableNameStack(Stack<String> tokTableNameStack) {
		this.tokTableNameStack = tokTableNameStack;
	}

	public Stack<String> getTokDbNameStack() {
		return tokDbNameStack;
	}

	public void setTokDbNameStack(Stack<String> tokDbNameStack) {
		this.tokDbNameStack = tokDbNameStack;
	}

	public Map<String, TableLineageInfo> getTableAliasLineageMap() {
		return tableAliasLineageMap;
	}

	public void setTableAliasLineageMap(Map<String, TableLineageInfo> tableAliasLineageMap) {
		this.tableAliasLineageMap = tableAliasLineageMap;
	}

	public Map<String, String> getTableReferAliasMap() {
		return tableReferAliasMap;
	}

	public void setTableReferAliasMap(Map<String, String> tableReferAliasMap) {
		this.tableReferAliasMap = tableReferAliasMap;
	}

	public String getCurrentTable() {
		return currentTable;
	}

	public void setCurrentTable(String currentTable) {
		this.currentTable = currentTable;
	}

	public Map<String, String> getFieldAliasMap() {
		return fieldAliasMap;
	}

	public void setFieldAliasMap(Map<String, String> fieldAliasMap) {
		this.fieldAliasMap = fieldAliasMap;
	}

	public List<String> getAliasFieldList() {
		return aliasFieldList;
	}

	public void setAliasFieldList(List<String> aliasFieldList) {
		this.aliasFieldList = aliasFieldList;
	}

	public List<String> getCleanFieldList() {
		return cleanFieldList;
	}

	public void setCleanFieldList(List<String> cleanFieldList) {
		this.cleanFieldList = cleanFieldList;
	}

	public Map<String, String> getTopLevelTableAliasMap() {
		return topLevelTableAliasMap;
	}

	public void setTopLevelTableAliasMap(Map<String, String> topLevelTableAliasMap) {
		this.topLevelTableAliasMap = topLevelTableAliasMap;
	}

	public Map<String, String> getMap1st() {
		return map1st;
	}

	public void setMap1st(Map<String, String> map1st) {
		this.map1st = map1st;
	}

	public List<ImmutablePair<String, String>> getPairList() {
		return pairList;
	}

	public void setPairList(List<ImmutablePair<String, String>> pairList) {
		this.pairList = pairList;
	}

	public static TableRelation parse(String sql) {
		ASTNode ast = null;
		try {
			ast = pd.parse(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(ast.toStringTree());

		HiveTableLineageParserClean parser=new HiveTableLineageParserClean();
		parser.parseIteral(ast);
		
		return parser.parseEnd();
	}
	
	private TableRelation parseEnd() {
		for (Entry<String, String> e : insertSelectFieldMap.entrySet()) {
			Set<String> s1 = SqlLineageUtil.addAliasName(e.getValue(), topLevelTableAliasMap);
			if (null != s1) {
				for (String i1 : s1) {
					Set<String> s2 = SqlLineageUtil.addAliasName2(i1, fieldAliasMap);
					if (null != s2) {
						for (String i2 : s2) {
							Set<String> s3 = SqlLineageUtil.addReferTableName(i2, tableAliasLineageMap);
							if (null != s3) {
								for (String i3 : s3) {
									map1st.put(e.getKey(), i3);
									pairList.add(new ImmutablePair<String, String>(e.getKey(), i3));
								}
							}
						}
					}
				}
			}
		}
		
		JamesUtil.printDivider("map1st");
		JamesUtil.printStringMap(map1st);
		JamesUtil.printDivider();

		Map<String, String> ultraFieldMap = new HashMap<String, String>();
		for (Entry<String, String> e : map1st.entrySet()) {
			ultraFieldMap.put(e.getKey(), SqlLineageUtil.replaceWithSrcTableName(e.getValue(), tableAliasLineageMap));
		}
		JamesUtil.printDivider("ultraFieldMap");
		JamesUtil.printStringMap(ultraFieldMap);

		TableRelation tableRelation = SqlLineageUtil.generateTableRelation(ultraFieldMap, "tgtTableName");
		JamesUtil.printDivider("tableRelation");
		System.out.println(tableRelation);
		
		return tableRelation;
	}

	public static void main(String[] args) throws IOException {
		String sqlDemo = "INSERT INTO TABLE f_tt SELECT at_a.a_a AS f_a_a, at_b.b_b AS f_b_b, at_b.b_c AS f_b_c FROM(SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b, MAX(a_c) AS a_c FROM t_a WHERE a_c = 3 GROUP BY a_key ORDER BY a_a) at_a LEFT JOIN (SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b, MAX(b_c) AS b_c FROM t_b GROUP BY b_key ORDER BY b_b) at_b ON at_a.a_key = at_b.b_key";
		String parsesql = sqlDemo;
		System.out.println(parsesql);

		JamesUtil.printDivider();
		TableRelation tableRelation =HiveTableLineageParserClean.parse(parsesql);
		
		SqlLineageUtil.makeGexf(tableRelation);
	}
}