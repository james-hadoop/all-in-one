package com.james.temp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import com.james.temp.SqlLineageUtil;
import com.james.temp.TableRelation;

public class HiveTableLineageParserBriefTemp8 {
	/*
	 * TableRelation
	 */
	private static List<TableNode> srcTables = new ArrayList<TableNode>();
	private static TableNode tgtTable = new TableNode();

	/*
	 * alias tables and fields
	 */
	private static Map<String, String> tableAliasMap = new HashMap<String, String>();
	private static Map<String, String> insertSelectFieldMap = new TreeMap<String, String>();

	/*
	 * 2 stacks for generating alias table map
	 */
	private static Stack<String> tokTableNameStack = new Stack<String>();
	private static Stack<String> tokDbNameStack = new Stack<String>();

	/*
	 * TableAliasEntity
	 */
	private static Map<String, TableLineageInfo> tableAliasLineageMap = new HashMap<String, TableLineageInfo>();
	private static Map<String, String> tableReferAliasMap = new HashMap<String, String>();

	private static String currentTable = "";
	private static Map<String, String> fieldAliasMap = new TreeMap<String, String>();

	private static List<String> aliasFieldList = new ArrayList<String>();
	private static List<String> cleanFieldList = new ArrayList<String>();

	private static Map<String, String> topLevelTableAliasMap = new HashMap<String, String>();

	// 1st round transform
	private static Map<String, String> map1st = new HashMap<String, String>();

	// ParseDriver pd
	private static ParseDriver pd = new ParseDriver();

	// 1st round transform
	private static List<ImmutablePair<String, String>> pairList = new ArrayList<ImmutablePair<String, String>>();

	// join tables stack
	private static Stack<String> joinTopLevelTableStackTemp = new Stack<String>();
	private static Set<String> joinTopLevelTableSet = new HashSet<String>();
	private static Map<String, ArrayList<String>> tableNameAliasMap = new HashMap<String, ArrayList<String>>();

	// parseCurrentNode
	private static void parseCurrentNode(ASTNode ast) {
		if (ast.getToken() != null) {
			switch (ast.getToken().getType()) {
			case HiveParser.TOK_LEFTOUTERJOIN:
				System.out.println("HiveParser.TOK_LEFTOUTERJOIN");

				if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_SUBQUERY) {
					String currentTableAliasName = ast.getChild(1).getChild(1).getText();
					ArrayList<String> tableAliasNameList = new ArrayList<String>();

					if (joinTopLevelTableStackTemp.size() > 0) {
						while (joinTopLevelTableStackTemp.size() > 0) {
							String tableName = joinTopLevelTableStackTemp.pop();
							tableAliasNameList.add(tableName);
						}
					}

					tableNameAliasMap.put(currentTableAliasName, tableAliasNameList);

					joinTopLevelTableStackTemp.push(currentTableAliasName);
					joinTopLevelTableSet.add(currentTableAliasName);
				} else {
					if (joinTopLevelTableStackTemp.size() > 0) {
						joinTopLevelTableSet.add(joinTopLevelTableStackTemp.pop());
					}
					joinTopLevelTableStackTemp.push(ast.getChild(0).getChild(1).getText());
					joinTopLevelTableStackTemp.push(ast.getChild(1).getChild(1).getText());
				}

				break;
			case HiveParser.TOK_JOIN:
				System.out.println("HiveParser.TOK_JOIN");
				if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_SUBQUERY) {
					String currentTableAliasName = ast.getChild(1).getChild(1).getText();
					ArrayList<String> tableAliasNameList = new ArrayList<String>();

					if (joinTopLevelTableStackTemp.size() > 0) {
						while (joinTopLevelTableStackTemp.size() > 0) {
							String tableName = joinTopLevelTableStackTemp.pop();
							tableAliasNameList.add(tableName);
						}
					}

					tableNameAliasMap.put(currentTableAliasName, tableAliasNameList);

					joinTopLevelTableStackTemp.push(currentTableAliasName);
					joinTopLevelTableSet.add(currentTableAliasName);
				} else {
					if (joinTopLevelTableStackTemp.size() > 0) {
						joinTopLevelTableSet.add(joinTopLevelTableStackTemp.pop());
					}

					joinTopLevelTableStackTemp.push(ast.getChild(0).getChild(1).getText());
					joinTopLevelTableStackTemp.push(ast.getChild(1).getChild(1).getText());
				}

				break;
			case HiveParser.TOK_FROM:
				String tokDbName = "";
				String tokTableName = "";
				if (ast.getChild(0).getChild(0).getChildCount() == 1) {
					// 不带库名
					tokTableName = ast.getChild(0).getChild(0).getChild(0).getText();
				} else if (ast.getChild(0).getChild(0).getChildCount() == 2) {
					// 带库名
					tokDbName = ast.getChild(0).getChild(0).getChild(0).getText();
					tokTableName = tokDbName + "." + ast.getChild(0).getChild(0).getChild(1).getText();
				}

				if (!tokTableName.contains("tok_unionall")) {
					// tokTablename does not contains "tok_unionall.a"
					tokDbNameStack.push(tokDbName);
					tokTableNameStack.push(tokTableName);
				}

				break;

			case HiveParser.TOK_SUBQUERY:
				if (ast.getChildCount() == 2) {
					String tableAlias = unescapeIdentifier(ast.getChild(1).getText());
					currentTable = tableAlias;

					tableAliasMap.put(tableAlias, tokTableNameStack.peek());

					System.out.println(
							"(*) " + tableAlias + " | " + tokDbNameStack.peek() + " | " + tokTableNameStack.peek());

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

							// TODO TableAliasEntity
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
								// TODO TableAliasEntity
								Map<String, String> aliasMap = new HashMap<String, String>();
								aliasMap.put(tableAlias, strToAdd);
								TableLineageInfo tableAliasEntity = new TableLineageInfo(tableAlias, aliasMap);
								tableAliasLineageMap.put(tableAlias, tableAliasEntity);
								tableReferAliasMap.put(strToAdd, tableAlias);
							} else {
								// TODO TableAliasEntity
								tableAliasLineageMap.get(tableAlias).getTableAliasReferMap().put(tableAlias, strToAdd);
								tableReferAliasMap.put(strToAdd, tableAlias);
							}

							topLevelTableAliasMap.put(tableAlias, tokDBAliasName);
						}
					} else {
						// if (tokDbNameStack.size()<=0)
						// System.out.println("tokDbNameStack.size()>0");
					}
				} else {
					// System.out.println("ast.getChildCount() ！= 2");
				}

				for (int i = 0; i < aliasFieldList.size(); i++) {
					fieldAliasMap.put(unescapeIdentifier(ast.getChild(1).getText()) + "." + aliasFieldList.get(i),
							cleanFieldList.get(i));
				}
				aliasFieldList.clear();
				cleanFieldList.clear();
				// System.out.println("TOK_SUBQUERY");

				break;

			case HiveParser.TOK_TABREF:// inputTable
				ASTNode tabTree = (ASTNode) ast.getChild(0);
				String tableName = (tabTree.getChildCount() == 1)
						? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
						: BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "."
								+ tabTree.getChild(1);
				srcTables.add(new TableNode(tableName));
				if (ast.getChild(1) != null) {
					String alia = ast.getChild(1).getText();
					tableAliasMap.put(alia, tableName);
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
						String tgtTableName = ast.getChild(0).getChild(1).getChild(0).getChild(0).getText();
						cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getText();
						aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

						System.out.print("currentTable:" + currentTable + "\t");
						System.out.println("字段别名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> "
								+ tgtTableName + "." + cleanFieldName);

					} else if (ast.getChild(0).getChild(0).getType() == HiveParser.KW_WHEN) {
						// System.out.println("HiveParser.KW_WHEN");
						if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_FUNCTION) {
							if (ast.getChild(0).getChild(1).getChild(0).getType() == HiveParser.TOK_ISNOTNULL) {
								if (ast.getChild(0).getChild(1).getChild(1).getType() == HiveParser.DOT) {
									/*
									 * case when N.level is not null then N.level else 2 end as level
									 *
									 * (tok_selexpr (tok_function when (tok_function tok_isnotnull (.
									 * (tok_table_or_col n) level)) (. (tok_table_or_col n) level) 2) level)
									 *
									 */
									cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getChild(1).getText()
											.toLowerCase();
									aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

								} else {
									System.out.println("unprocessed situation_1");
								}

							} else if (ast.getChild(0).getChild(1).getChild(0).getType() == HiveParser.TOK_ISNULL) {
								if (ast.getChild(0).getChild(1).getChild(1).getType() == HiveParser.DOT) {
									cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getChild(1).getText()
											.toLowerCase();
									aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

								} else {
									System.out.println("unprocessed situation_2");
								}
							} else {
								/*
								 * (tok_selexpr (tok_function when (tok_function in (tok_table_or_col source)
								 * '1' '3') 1 0) is_kd_source)
								 */
								cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getChild(0).getText()
										.toLowerCase();
								aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;
							}
						} else if (ast.getChild(0).getChild(1).getType() == HiveParser.EQUAL) {
							/*
							 * (tok_selexpr (tok_function when (= (tok_table_or_col source) 'hello') 1 0)
							 * s_kd_source)
							 */
							cleanFieldName = ast.getChild(0).getChild(1).getChild(0).getChild(0).getText()
									.toLowerCase();
							aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;
						} else if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_WINDOWSPEC) {
							// TODO HiveParser.TOK_WINDOWSPEC
						} else {
							// TODO
							System.out.println("unprocessed situation_3");
						}
					} else if (ast.getChild(0).getChild(0).getText().toLowerCase().equals("sum")) {
						if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_FUNCTION) {
							System.out.println("sum");
							if (ast.getChild(0).getChild(1).getChild(0).getType() == HiveParser.KW_WHEN) {
								if (ast.getChild(0).getChild(1).getChild(1).getType() == HiveParser.EQUAL) {
									cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getChild(0).getChild(0)
											.getText().toLowerCase();
									aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;
								}
							}

						} else {
							System.out.println("unprocessed situation_sum");
						}

					} else {
						// TODO
						System.out.println("unprocessed situation_4");
					}
				} else if (ast.getChild(0).getType() == HiveParser.DOT) {
					if (ast.getChild(0).getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
						String tgtTableName = ast.getChild(0).getChild(0).getChild(0).getText();
						cleanFieldName = ast.getChild(0).getChild(1).getText().toLowerCase();
						aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

						System.out.print("currentTable:" + currentTable + "\t");
						System.out.println("字段别名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> "
								+ tgtTableName + "." + cleanFieldName);

					}
				} else if (ast.getChild(0).getType() == HiveParser.TOK_FUNCTIONDI) {
					if (ast.getChild(0).getChild(1).getChildCount() == 1) {
						cleanFieldName = ast.getChild(0).getChild(1).getChild(0).getText().toLowerCase();
						aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;

						System.out.print("currentTable:" + currentTable + "\t");
						System.out.println(
								"字段別名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> " + cleanFieldName);

					} else {

						cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getText().toLowerCase();
						aliasFieldName = null == aliasFieldName ? cleanFieldName : aliasFieldName;
					}
					System.out.print("currentTable:" + currentTable + "\t");
					System.out.println(
							"字段別名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> " + cleanFieldName);
				} else {
					// TODO
					System.out.println("unprocessed situation_5");
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
					if (astNode.getChild(i).getChildCount() == 1) {
						// (tok_selexpr 20190226) do nothing
					} else if (astNode.getChild(i).getChildCount() == 2
							&& astNode.getChild(i).getChild(0).getChildCount() == 0) {
						// (tok_selexpr 'aaaaa' s_a)
						String fieldCleanName = astNode.getChild(i).getChild(1).getText().toLowerCase();
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
							System.out.println("HiveParser.KW_WHEN");

							if (astNode.getChild(i).getChild(0).getChild(1).getType() == HiveParser.TOK_FUNCTION) {

								if (astNode.getChild(i).getChildCount() == 1) {
									/*
									 * (tok_selexpr (tok_function when (tok_function tok_isnotnull (.
									 * (tok_table_or_col y) article_uv)) (. (tok_table_or_col y) article_uv) 0))
									 */
									System.out.println("astNode.getChild(i).getChildCount()==1");
								} else if (astNode.getChild(i).getChild(0).getChild(1).getChild(0)
										.getType() == HiveParser.TOK_ISNOTNULL) {
									System.out.println(
											"astNode.getChild(i).getChild(0).getChild(1).getChild(1).getType()==HiveParser.TOK_ISNOTNULL");
									// z table
									// TODO
									if (astNode.getChild(i).getChild(0).getChild(1).getChild(1)
											.getType() == HiveParser.DOT) {
										String fieldCleanName = astNode.getChild(i).getChild(0).getChild(1).getChild(1)
												.getChild(1).getText().toLowerCase();
										String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
										insertSelectFieldMap.put(filedAliasName, fieldCleanName);
									}
								} else if (null != astNode.getChild(i).getChild(1).getChild(1)
										&& astNode.getChild(i).getChild(1).getChild(1).getChildCount() > 1) {
									/*
									 * (tok_selexpr (tok_function when (tok_function in (tok_table_or_col source)
									 * '1' '3') 1 0) is_kd_source)
									 */

									String fieldCleanName = ast.getChild(0).getChild(1).getChild(1).getChild(1)
											.getChild(1).getChild(0).getText().toLowerCase();
									String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
									insertSelectFieldMap.put(filedAliasName, fieldCleanName);
									// }
								} else if (astNode.getChild(i).getChild(0).getChild(1).getType() == HiveParser.EQUAL) {
									// (tok_selexpr (tok_function when (= (tok_table_or_col source) 'hello') 1 0)
									// s_kd_source)
									String fieldCleanName = astNode.getChild(i).getChild(0).getChild(1).getChild(0)
											.getChild(0).getText().toLowerCase();
									String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
									insertSelectFieldMap.put(filedAliasName, fieldCleanName);
								} else {
									String fieldCleanName = astNode.getChild(i).getChild(0).getChild(1).getChild(1)
											.getChild(0).getText().toLowerCase();
									String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
									insertSelectFieldMap.put(filedAliasName, fieldCleanName);
								}
							}
						}
						// else if() {
						//
						// }
					} else if (astNode.getChild(i).getChild(0).getType() == HiveParser.DIVIDE) {
						if (astNode.getChild(i).getChild(0).getChild(0).getType() == HiveParser.STAR) {
							System.out.println("HiveParser.STAR...");
							if (astNode.getChild(i).getChild(0).getChild(0).getChild(0).getChildCount() == 4) {
								String fieldCleanName = astNode.getChild(i).getChild(0).getChild(0).getChild(0)
										.getChild(2).getChild(1).getText().toLowerCase();
								// do nothing
							}

						}
						System.out.println("HiveParser.DIVIDE...");
					} else if (astNode.getChild(i).getChild(0).getType() == HiveParser.PLUS) {
						System.out.println("HiveParser.PLUS...");
					} else {
						System.out.println(astNode.getChild(i).getChild(0).getType());
						System.out.println("INSERT_INTO unporcessed situation...");

					}
				}
				break;
			} // for
		}
	}

	private static void parseIteral(ASTNode ast) {
		parseChildNodes(ast);
		parseCurrentNode(ast);
	}

	private static void parseChildNodes(ASTNode ast) {
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

	public static String unescapeIdentifier(String val) {
		if (val == null) {
			return null;
		}
		if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
			val = val.substring(1, val.length() - 1);
		}
		return val;
	}

	public static void parse(ASTNode ast) {
		parseIteral(ast);
	}

	public static void main(String[] args) throws IOException {
		// ParseDriver pd = new ParseDriver();

		String sqlDemo = "INSERT INTO TABLE f_tt SELECT at_a.a_a AS f_a_a, at_b.b_b AS f_b_b, at_b.b_c AS f_b_c FROM(SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b, MAX(a_c) AS a_c FROM t_a WHERE a_c = 3 GROUP BY a_key ORDER BY a_a) at_a LEFT JOIN (SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b, MAX(b_c) AS b_c FROM t_b GROUP BY b_key ORDER BY b_b) at_b ON at_a.a_key = at_b.b_key";
		String sql52 = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_a.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
		String sql52_b = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_b.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
		
		String parsesql = sqlDemo;
		System.out.println(parsesql);

		// HiveTableLineageParserBriefTemp6 hp = new HiveTableLineageParserBriefTemp6();

		ASTNode ast = null;
		try {
			ast = pd.parse(parsesql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(ast.toStringTree());

		JamesUtil.printDivider();
		HiveTableLineageParserBriefTemp8.parse(ast);

		JamesUtil.printDivider();
		System.out.println(tgtTable.getTableName());
		for (TableNode t : srcTables) {
			System.out.println(t.getTableName());
		}

		JamesUtil.printDivider("insertSelectFieldMap");
		JamesUtil.printStringMap(insertSelectFieldMap);

		for (Entry<String, String> entry : insertSelectFieldMap.entrySet()) {
			System.out
					.println(entry.getValue() + " -> " + SqlLineageUtil.findSrcField(entry.getValue(), fieldAliasMap));
		}

		JamesUtil.printDivider("tokDbNameStack");
		JamesUtil.printStack(tokDbNameStack);
		JamesUtil.printDivider("tokTableNameStack");
		JamesUtil.printStack(tokTableNameStack);

		JamesUtil.printDivider("tableReferAliasMap");
		JamesUtil.printStringMap(tableReferAliasMap);

		JamesUtil.printDivider("tableAliasLineageMap");
		Collection<TableLineageInfo> list = tableAliasLineageMap.values();
		for (TableLineageInfo e : list) {
			System.out.println(e);
		}

		JamesUtil.printDivider("fieldAliasMap");
		JamesUtil.printStringMap(fieldAliasMap);

		JamesUtil.printDivider("topLevelTableAliasMap");
		JamesUtil.printStringMap(topLevelTableAliasMap);

		JamesUtil.printDivider("output");

		// If using "select *", the insertSelectFieldMap will be empty
		// add the fields into insertSelectFieldMap with ones belong to
		// topLevelTableAliasMap
		if (0 == insertSelectFieldMap.size()) {
			Set<String> set = topLevelTableAliasMap.keySet();
			String outerTable = null;
			for (String k : set) {
				outerTable = k;
			}

			Set<String> aliasFieldSet = fieldAliasMap.keySet();
			for (String k : aliasFieldSet) {
				if (k.contains(outerTable)) {
					insertSelectFieldMap.put(k, fieldAliasMap.get(k));
				}
			}
		}

		for (Entry<String, String> e : insertSelectFieldMap.entrySet()) {
			System.out.println("key=" + e.getKey());

			Set<String> s1 = SqlLineageUtil.addAliasName3(e.getValue(), joinTopLevelTableSet);
			if (null != s1) {
				for (String i1 : s1) {
					// System.out.println("\t" + i1);
					// System.out.println("-------- i1 --------");
					Set<String> s2 = SqlLineageUtil.addAliasName2(i1, fieldAliasMap);
					if (null != s2) {
						for (String i2 : s2) {
							// System.out.println("\t" + i2);
							// System.out.println("-------- i2 --------");
							Set<String> s3 = SqlLineageUtil.addReferTableName(i2, tableAliasLineageMap);
							if (null != s3) {
								for (String i3 : s3) {
									System.out.println("\t" + i3);
									System.out.println("-------- i3 --------");

									map1st.put(e.getKey(), i3);
									pairList.add(new ImmutablePair<String, String>(e.getKey(), i3));
								}
							}
						}
					}
				}
			}
			System.out.println();
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

		SqlLineageUtil.makeGexf(tableRelation);

		JamesUtil.printDivider("20190411");
		JamesUtil.printDivider("tableAliasMap");
		JamesUtil.printStringMap(tableAliasMap);
		JamesUtil.printDivider("joinTopLevelTableSet");
		if (joinTopLevelTableStackTemp.size() > 0) {
			while (joinTopLevelTableStackTemp.size() > 0) {
				String tableName = joinTopLevelTableStackTemp.pop();
				joinTopLevelTableSet.add(tableName);
			}
		}
		JamesUtil.printSet(joinTopLevelTableSet);
		JamesUtil.printDivider("tableNameAliasMap");
		for (Entry<String, ArrayList<String>> entry : tableNameAliasMap.entrySet()) {
			System.out.println(entry.getKey());
			for (String s : entry.getValue()) {
				System.out.println("\t" + s);
			}
		}

		JamesUtil.printDivider("flattenTableNameAliasSet");
		List<ImmutablePair<String, String>> flattenedPairList = new ArrayList<ImmutablePair<String, String>>();
		Map<String, String> retMap = SqlLineageUtil.flattenTableNameAliasMap(joinTopLevelTableSet, tableNameAliasMap,
				flattenedPairList, null);

		Map<String, HashSet<String>> flattenedTopLevelTableAliasMap = new HashMap<String, HashSet<String>>();
		for (ImmutablePair<String, String> pair : flattenedPairList) {
			if (joinTopLevelTableSet.contains(pair.getRight())) {
				continue;
			}

			if (null == flattenedTopLevelTableAliasMap.get(pair.getLeft())) {
				HashSet<String> set = new HashSet<String>();
				set.add(pair.getRight());

				flattenedTopLevelTableAliasMap.put(pair.getLeft(), set);
			} else {
				flattenedTopLevelTableAliasMap.get(pair.getLeft()).add(pair.getRight());
			}
		}

		System.out.println(flattenedPairList);

		JamesUtil.printDivider("flattenedTopLevelTableAliasMap");
		System.out.println(flattenedTopLevelTableAliasMap);
		
		JamesUtil.printDivider("2019-04-12");
		
	}
}
