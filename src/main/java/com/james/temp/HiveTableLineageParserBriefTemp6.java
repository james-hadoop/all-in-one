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

public class HiveTableLineageParserBriefTemp6 {
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

	// parseCurrentNode
	private static void parseCurrentNode(ASTNode ast) {
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

				if (!tokTableName.contains("tok_unionall")) {
					// tokTablename does not contains "tok_unionall.a"
					tokDbNameStack.push(tokDbName.toLowerCase());
					tokTableNameStack.push(tokTableName.toLowerCase());
				}

				break;

			case HiveParser.TOK_SUBQUERY:
				if (ast.getChildCount() == 2) {
					String tableAlias = unescapeIdentifier(ast.getChild(1).getText()).toLowerCase();
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
//						System.out.println("tokDbNameStack.size()>0");
					}
				} else {
//					System.out.println("ast.getChildCount() ！= 2");
				}

				for (int i = 0; i < aliasFieldList.size(); i++) {
					fieldAliasMap.put(
							unescapeIdentifier(ast.getChild(1).getText()).toLowerCase() + "." + aliasFieldList.get(i),
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
							// TODO
							System.out.println("unprocessed situation");
						}
					} else {
						// TODO
						System.out.println("unprocessed situation");
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
					// TODO
					System.out.println("unprocessed situation");
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
								// (tok_selexpr (tok_function when (tok_function in (tok_table_or_col source)
								// '1' '3') 1 0) is_kd_source)
								if (null != astNode.getChild(i).getChild(1).getChild(1)
										&& astNode.getChild(i).getChild(1).getChild(1).getChildCount() > 1) {
									String fieldCleanName = ast.getChild(0).getChild(1).getChild(1).getChild(1)
											.getChild(1).getChild(0).getText().toLowerCase();
									String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
									insertSelectFieldMap.put(filedAliasName, fieldCleanName);
								}
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
				}
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
//		ParseDriver pd = new ParseDriver();

		String sqlDemo = "INSERT INTO TABLE f_tt SELECT at_a.a_a AS f_a_a, at_b.b_b AS f_b_b, at_b.b_c AS f_b_c FROM(SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b, MAX(a_c) AS a_c FROM t_a WHERE a_c = 3 GROUP BY a_key ORDER BY a_a) at_a LEFT JOIN (SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b, MAX(b_c) AS b_c FROM t_b GROUP BY b_key ORDER BY b_b) at_b ON at_a.a_key = at_b.b_key";
		String sql52 = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_a.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
		String sql52_b = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_b.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
		String sql61 = "INSERT INTO TABLE t_kandian_account_video_uv_daily_new SELECT 20190226, 'aaaaa' AS s_a, C.puin puin , C.row_key , CASE WHEN SOURCE IN('1' ,'3') THEN 1 ELSE 0 END AS is_kd_source , CASE WHEN SOURCE='hello' THEN 1 ELSE 0 END AS s_kd_source , uv, vv a_vv, c.uv c_uv, d.puin d_puin FROM(SELECT puin , A.row_key , COUNT(DISTINCT A.cuin) AS uv , SUM(A.vv) AS vv FROM (SELECT case when rowkey=\"AAA\" then 'yes' else 'no' end rettt,cuin , business_id AS puin , op_cnt AS vv , rowkey AS row_key , RANK() OVER (PARTITION BY rowkey ORDER BY ftime) AS f_rank FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate = 20190226 AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ('ab' , 'ae' , 'af' , 'aj' , 'al' , 'ao') AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100) A LEFT JOIN (SELECT MAX(fdate) AS tdbank_imp_date , rowkey AS row_key , SUM(op_cnt) AS history_vv FROM sng_cp_fact.v_ty_BBBB WHERE fdate BETWEEN DATE_SUB(20190226, 90) AND DATE_SUB(20190226, 1) AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ('ab' , 'ae' , 'af' , 'aj' , 'al' , 'ao') AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 GROUP BY rowkey) B ON A.row_key = B.row_key WHERE ((B.history_vv IS NOT NULL AND f_rank < (3000001 - B.history_vv)) OR (f_rank < 3000001 AND B.history_vv IS NULL)) GROUP BY A.puin , A.row_key) C LEFT JOIN (SELECT puin , row_key , CASE WHEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') IS NOT NULL THEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') ELSE GET_JSON_OBJECT(MAX(extra_info), '$.src') END AS SOURCE FROM sng_tdbank . cc_dsl_content_center_rpt_fdt0 WHERE tdbank_imp_date BETWEEN DATE_SUB(20190226, 90) AND 20190226 AND op_type = '0XCC0V000' AND GET_JSON_OBJECT(extra_info, '$.renewal') NOT IN ('1') AND src IN ('2' , '5' , '6' , '10' , '12' , '15') GROUP BY puin , row_key) D ON C.row_key = D.row_key";
		String sql71 = "INSERT INTO TABLE sng_mp_etldata.t_kandian_tuwen_tuji_hourly SELECT * from(SELECT 2019030919 AS tdbank_imp_date, md5(A.uin,'kd_uin') AS uin, A.rowkey, B.ex_id, A.op_type, A.time,A.os,A.imei,A.idfa,A.imsi,A.op_cnt, 0 AS pic_cnt, 2 AS TYPE FROM (SELECT uin,op_type, get_json_object(d4,'$.rowkey') AS rowkey, regexp_replace(reporttime,'[^0-9]','') AS time, get_json_object(d4,'$.os') AS os, get_json_object(d4,'$.imei') AS imei, get_json_object(d4,'$.idfa') AS idfa, get_json_object(d4,'$.imsi') AS imsi, op_cnt FROM hlw.t_dw_dc01160 WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X8007625','0X8007626') AND substr(get_json_object(d4,'$.rowkey'),15,16) IN ('26','50','51','52','53','54','55','56','57','58','59') UNION ALL SELECT md5(uin,'kd_uin') AS uin,op_type, get_json_object(extra_info,'$.rowkey') AS rowkey, time, get_json_object(extra_info,'$.os') AS os, get_json_object(extra_info,'$.imei') AS imei, get_json_object(extra_info,'$.idfa') AS idfa, get_json_object(extra_info,'$.imsi') AS imsi, 1 AS op_cnt FROM sng_mp_etldata.t_mp_article_click_table_hourly WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X800662D','0X800662E') AND substr(get_json_object(extra_info,'$.rowkey'),15,16) IN ('26','50','51','52','53','54','55','56','57','58','59')) A LEFT JOIN (SELECT rowkey,ex_id FROM sng_mp_etldata.t_article_hbase_hourly_limit WHERE src IN ('26','49','50','51','52','53','54','55','56','57','58','59') AND imp_date = 2019030919) B ON A.rowkey = B.rowkey UNION ALL SELECT 2019030919 AS tdbank_imp_date, C.uin, C.rowkey, D.ex_id, C.op_type, C.time, NULL AS os, C.imei, C.idfa, C.imsi, C.op_cnt, C.pic_cnt, 1 AS TYPE FROM (SELECT md5('',uin) AS uin, op_type, get_json_object(d4,'$.rowkey') AS rowkey, reporttime AS time, imei, get_json_object(d4,'$.idfa') AS idfa, get_json_object(d4,'$.imsi') AS imsi, op_cnt, CASE WHEN op_type = '0X8008E30' THEN size(split(get_json_object(d4,'$.one_pic_reported'),'\\\\\\}\\\\\\,\\\\\\{')) ELSE 0 END AS pic_cnt FROM hlw.t_dw_dc01160 WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X8007625', '0X8007626', '0X8008E30') AND substr(get_json_object(d4,'$.rowkey'),15,2)='49')C JOIN (SELECT rowkey,ex_id FROM sng_mp_etldata.t_article_hbase_hourly_limit WHERE src = 49 AND sub_src = '490002' AND imp_date = 2019030919)D ON C.rowkey = D.rowkey) t_outer";

		String parsesql = sql61;
		System.out.println(parsesql);

//		HiveTableLineageParserBriefTemp6 hp = new HiveTableLineageParserBriefTemp6();

		ASTNode ast = null;
		try {
			ast = pd.parse(parsesql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(ast.toStringTree());

		JamesUtil.printDivider();
		HiveTableLineageParserBriefTemp6.parse(ast);

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

			Set<String> s1 = SqlLineageUtil.addAliasName(e.getValue(), topLevelTableAliasMap);
			if (null != s1) {
				for (String i1 : s1) {
//					System.out.println("\t" + i1);
//					System.out.println("-------- i1 --------");
					Set<String> s2 = SqlLineageUtil.addAliasName2(i1, fieldAliasMap);
					if (null != s2) {
						for (String i2 : s2) {
//							System.out.println("\t" + i2);
//							System.out.println("-------- i2 --------");
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
	}
}