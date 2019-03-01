package com.james.temp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import com.james.common.util.JamesUtil;

public class HiveTableLineageParserBriefTemp2 {
	/*
	 * TableRelation
	 */
	private static List<TableNode> srcTables = new ArrayList<TableNode>();
	private static TableNode tgtTable = new TableNode();

	/*
	 * alias tables and fields
	 */
	private static Map<String, String> tableAliasMap = new HashMap<String, String>();
	private static Map<String, String> fieldAliasMap = new TreeMap<String, String>();
	private static Map<String, String> insertSelectFieldMap = new TreeMap<String, String>();

	/*
	 * 2 stacks for generating alias table map
	 */
	private static Stack<String> tokTableNameStack = new Stack<String>();
	private static Stack<String> tokDbNameStack = new Stack<String>();
	private static Map<String, Set<String>> tableAliasSetMap = new HashMap<String, Set<String>>();

	/*
	 * tables存入的是每个表名以及表名对应的操作 String = tableName + "\t" + OPER
	 */
//	private Set<String> tables = new HashSet<String>();
//	private Stack<String> tableAliasNameStack = new Stack<String>();
	private Stack<Oper> operStack = new Stack<Oper>();
	private Oper oper;

	private enum Oper {
		SELECT, INSERT, TRUNCATE, LOAD, CREATE＿TABLE, ALTER, DROP_TABLE, SHOW, DELETE, UPDATE, DESC
	}

	// parseCurrentNode
	private Set<String> parseCurrentNode(ASTNode ast, Set<String> set) {
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
					tokTableName = ast.getChild(0).getChild(0).getChild(1).getText().toLowerCase();
				}

				tokDbNameStack.push(tokDbName.toLowerCase());
				tokTableNameStack.push(tokTableName.toLowerCase());
				break;

			case HiveParser.TOK_SUBQUERY:
				if (ast.getChildCount() == 2) {
					String tableAlias = unescapeIdentifier(ast.getChild(1).getText()).toLowerCase();
					String aliaReal = "";
					for (String table : set) {
						aliaReal += table + "&";
					}
					if (aliaReal.length() != 0) {
						aliaReal = aliaReal.substring(0, aliaReal.length() - 1);
					}

					tableAliasMap.put(tableAlias.toLowerCase(), aliaReal);

					if (tokDbNameStack.size() > 0) {
						String tokDBAliasName = tokDbNameStack.peek();
						String tokTableAliasName = tokTableNameStack.peek();

						if (tokDBAliasName.equals("tok_query")) {
							tokDbNameStack.pop();
							tokTableNameStack.pop();

							Set<String> tableAliasSet = new HashSet<String>();
							do {
								tableAliasSet.add(tokDbNameStack.pop() + "." + tokTableNameStack.pop());
							} while (tokDbNameStack.size() > 0);

							tableAliasSetMap.put(tableAlias, tableAliasSet);
						} else {
							String strToAdd = tokDBAliasName + "." + tokTableAliasName;

							if (null == tableAliasSetMap.get(tableAlias)) {
								Set<String> tableAliasSet = new HashSet<String>();
								tableAliasSet.add(strToAdd);
								tableAliasSetMap.put(tableAlias, tableAliasSet);
							} else {
								tableAliasSetMap.get(tableAlias).add(strToAdd);
							}
						}
					} // if (tokDbNameStack.size()>0)
				}
				break;
			case HiveParser.TOK_TABREF:// inputTable
				ASTNode tabTree = (ASTNode) ast.getChild(0);
				String tableName = (tabTree.getChildCount() == 1)
						? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
						: BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "."
								+ tabTree.getChild(1);
				if (oper == Oper.SELECT) {
					set.add(tableName);
				}
				srcTables.add(new TableNode(tableName));
				if (ast.getChild(1) != null) {
					String alia = ast.getChild(1).getText().toLowerCase();
					tableAliasMap.put(alia.toLowerCase(), tableName);// sql6 p别名在tabref只对应为一个表的别名。
				}
				break;
			case HiveParser.TOK_SELEXPR:
				// TODO CASE WHEN

				String fieldName = "";
				String cleanFieldName = "";
				String aliasFieldName = "";

				if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
					fieldName = ast.getChild(0).getChild(0).getText().toLowerCase();
					aliasFieldName = null == ast.getChild(1) ? fieldName : ast.getChild(1).getText().toLowerCase();

					System.out.println("字段別名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> " + fieldName);
					fieldAliasMap.put(tokTableNameStack.peek() + "." + aliasFieldName, fieldName);
				} else if (ast.getChild(0).getType() == HiveParser.TOK_FUNCTION) {
					if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_TABLE_OR_COL) {
						fieldName = ast.getChild(0).getChild(0).getText() + "("
								+ ast.getChild(0).getChild(1).getChild(0).getText() + ")";
						cleanFieldName = ast.getChild(0).getChild(1).getChild(0).getText();
						aliasFieldName = null == ast.getChild(1) ? cleanFieldName
								: ast.getChild(1).getText().toLowerCase();

						System.out.println(
								"字段別名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> " + cleanFieldName);
						fieldAliasMap.put(tokTableNameStack.peek() + "." + aliasFieldName, cleanFieldName);

					} else if (ast.getChild(0).getChild(1).getType() == HiveParser.DOT) {
						String tgtTableName = ast.getChild(0).getChild(1).getChild(0).getChild(0).getText()
								.toLowerCase();
						String tgtCleanFieldName = ast.getChild(0).getChild(1).getChild(1).getText();

						String tgtAliasFieldName = ast.getChild(1) == null ? tgtCleanFieldName
								: ast.getChild(1).getText().toLowerCase();

						System.out.println("字段别名: " + tokTableNameStack.peek() + "." + tgtAliasFieldName + " -> "
								+ tgtTableName + "." + tgtCleanFieldName);
						fieldAliasMap.put(tokTableNameStack.peek() + "." + tgtAliasFieldName,
								tgtTableName + "." + tgtCleanFieldName);

					}

				} else if (ast.getChild(0).getType() == HiveParser.DOT) {
					if (ast.getChild(0).getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
						String tgtTableName = ast.getChild(0).getChild(0).getChild(0).getText().toLowerCase();
						String tgtFieldName = ast.getChild(0).getChild(1).getText().toLowerCase();
						String tgtAliasFieldName = ast.getChild(1) == null ? tgtFieldName
								: ast.getChild(1).getText().toLowerCase();

						System.out.println("字段别名: " + tokTableNameStack.peek() + "." + tgtAliasFieldName + " -> "
								+ tgtTableName + "." + tgtFieldName);
						fieldAliasMap.put(tokTableNameStack.peek() + "." + tgtAliasFieldName,
								tgtTableName + "." + tgtFieldName);

					}
				} else if (ast.getChild(0).getType() == HiveParser.TOK_FUNCTIONDI) {
					cleanFieldName = ast.getChild(0).getChild(1).getChild(1).getText().toLowerCase();
					aliasFieldName = ast.getChild(1).getText().toLowerCase();

					System.out.println(
							"字段別名: " + tokTableNameStack.peek() + "." + aliasFieldName + " -> " + cleanFieldName);
					fieldAliasMap.put(tokTableNameStack.peek() + "." + aliasFieldName, cleanFieldName);
				}
				break;
			case HiveParser.TOK_INSERT_INTO:
				ASTNode astNode = (ASTNode) ast.getParent().getChild(1);
				int nodeCount = ast.getParent().getChild(1).getChildCount();

				for (int i = 0; i < nodeCount; i++) {
					if (astNode.getChild(i).getChildCount() == 1
							&& astNode.getChild(i).getChild(0).getChildCount() == 0) {
						// (tok_selexpr 20190226)
						// do nothing
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
								String fieldCleanName = astNode.getChild(i).getChild(0).getChild(1).getChild(1)
										.getChild(0).getText().toLowerCase();
								String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
								insertSelectFieldMap.put(filedAliasName, fieldCleanName);
							} else if (astNode.getChild(i).getChild(0).getChild(1).getType() == HiveParser.EQUAL) {
								// (tok_selexpr (tok_function when (= (tok_table_or_col source) 'hello') 1 0)
								// s_kd_source)
								String fieldCleanName = astNode.getChild(i).getChild(0).getChild(1).getChild(0)
										.getChild(0).getText().toLowerCase();
								String filedAliasName = astNode.getChild(i).getChild(1).getText().toLowerCase();
								insertSelectFieldMap.put(filedAliasName, fieldCleanName);
							}
						}
					}
				}
				break;
		}

		if (ast.getToken() != null && ast.getToken().getType() >= HiveParser.TOK_SHOWCOLUMNS
				&& ast.getToken().getType() <= HiveParser.TOK_SHOW_TBLPROPERTIES)

		{
			ASTNode dropNode = (ASTNode) ast.getChild(0);
			String dropTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) dropNode);
		}
		if (ast.getToken() != null && ast.getToken().getType() >= HiveParser.TOK_DESCDATABASE
				&& ast.getToken().getType() <= HiveParser.TOK_DESCTABLE) {
			ASTNode descNode = (ASTNode) ast.getChild(0);
			if (!(descNode.getToken().getType() == HiveParser.TOK_COL_NAME)
					|| !(descNode.getToken().getType() == HiveParser.TOK_TABNAME)) {
				if (descNode.getChildCount() > 0) {
					descNode = (ASTNode) descNode.getChild(0);
				}
			}
			String descTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) descNode);
		}
		}
		return set;
	}

	public Set<String> parseIteral(ASTNode ast) {
		Set<String> set = new HashSet<String>();
//		prepareToParseCurrentNodeAndChilds(ast);
		set.addAll(parseChildNodes(ast));
		set.addAll(parseCurrentNode(ast, set));
//		endParseCurrentNode(ast);
		return set;
	}

	private void endParseCurrentNode(ASTNode ast) {
		if (ast.getToken() != null) {
			switch (ast.getToken().getType()) {
			case HiveParser.TOK_INSERT:
			case HiveParser.TOK_SELECT:
				oper = operStack.pop();
				break;
			}
		}
	}

	private Set<String> parseChildNodes(ASTNode ast) {
		Set<String> set = new HashSet<String>();
		int numCh = ast.getChildCount();
		if (numCh > 0) {
			for (int num = 0; num < numCh; num++) {
				ASTNode child = (ASTNode) ast.getChild(num);
				set.addAll(parseIteral(child));
			}
		}
		return set;
	}

	/**
	 * 
	 * @param ast
	 */
	private void prepareToParseCurrentNodeAndChilds(ASTNode ast) {
		if (ast.getToken() != null) {
			switch (ast.getToken().getType()) {
			case HiveParser.TOK_QUERY:
				operStack.push(oper);
				oper = Oper.SELECT;
				break;
			case HiveParser.TOK_INSERT:
				operStack.push(oper);
				oper = Oper.INSERT;
				break;
			case HiveParser.TOK_DELETE_FROM:
				operStack.push(oper);
				oper = Oper.DELETE;
				break;
			case HiveParser.TOK_UPDATE_TABLE:
				operStack.push(oper);
				oper = Oper.UPDATE;
				break;
			case HiveParser.TOK_SELECT:
				operStack.push(oper);
				oper = Oper.SELECT;
				break;
			case HiveParser.TOK_CREATETABLE:
				oper = Oper.CREATE＿TABLE;
				operStack.push(oper);
				break;
			}
			if (ast.getToken() != null && ast.getToken().getType() >= HiveParser.TOK_ALTERDATABASE_PROPERTIES
					&& ast.getToken().getType() <= HiveParser.TOK_ALTERVIEW_RENAME) {
				oper = Oper.ALTER;
			}
			if (ast.getToken() != null && ast.getToken().getType() >= HiveParser.TOK_SHOWCOLUMNS
					&& ast.getToken().getType() <= HiveParser.TOK_SHOW_TBLPROPERTIES) {
				oper = Oper.SHOW;
			}

			if (ast.getToken().getType() >= HiveParser.TOK_DESCDATABASE
					&& ast.getToken().getType() <= HiveParser.TOK_DESCTABLE) {
				oper = Oper.DESC;
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

	private void output(Map<String, String> map) {
		java.util.Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			System.out.println(key + " -> " + map.get(key));
		}
	}

	public void parse(ASTNode ast) {
		parseIteral(ast);

		System.out.println("***************表别名***************");
		output(tableAliasMap);
		System.out.println("***************字段别名***************");
		output(fieldAliasMap);
		System.out.println("***************表***************");
//		for (String table : tables) {
//			System.out.println(table);
//		}
	}

	public static void main(String[] args) {
		ParseDriver pd = new ParseDriver();
		String sql52 = "INSERT INTO TABLE t_target \r\n"
				+ "SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_a.same FROM (\r\n"
				+ "SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a \r\n"
				+ "LEFT JOIN (\r\n"
				+ "SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key";

		String sql52_a = "SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc";
		String sql52_b = "INSERT INTO TABLE t_target SELECT MAX(r_t_a.r_a_a) AS f_a_a, r_t_b.r_b_b AS f_b_b, MAX(r_t_a.r_a_c) AS f_a_c FROM( SELECT a_key, a_a AS r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c , MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a DESC) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b , MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b ) r_t_b ON r_t_a.a_key = r_t_b.b_key";
		String sql54 = "INSERT INTO TABLE t_kandian_account_video_uv_daily_new SELECT 20190226,'aaaaa' as s_a, C.puin puin ,C.row_key ,CASE WHEN source IN( '1' ,'3') THEN 1 ELSE 0 END AS is_kd_source ,CASE WHEN source='hello' THEN 1 ELSE 0 END AS s_kd_source ,uv,vv a_vv,c.uv c_uv FROM ( SELECT puin ,A.row_key ,COUNT(DISTINCT A.cuin) AS uv ,SUM(A.vv) AS vv FROM ( SELECT cuin ,business_id AS puin ,op_cnt AS vv ,rowkey AS row_key ,RANK() OVER ( PARTITION BY rowkey ORDER BY ftime ) AS f_rank FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate = 20190226 AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ( 'ab' ,'ae' ,'af' ,'aj' ,'al' ,'ao' ) AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 ) A LEFT JOIN ( SELECT MAX(fdate) AS tdbank_imp_date ,rowkey AS row_key ,SUM(op_cnt) AS history_vv FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate BETWEEN DATE_SUB(20190226, 90) AND DATE_SUB(20190226, 1) AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ( 'ab' ,'ae' ,'af' ,'aj' ,'al' ,'ao' ) AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 GROUP BY rowkey ) B ON A.row_key = B.row_key WHERE ( ( B.history_vv IS NOT NULL AND f_rank < (3000001 - B.history_vv) ) OR ( f_rank < 3000001 AND B.history_vv IS NULL ) ) GROUP BY A.puin ,A.row_key ) C LEFT JOIN ( SELECT puin ,row_key ,CASE WHEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') IS NOT null THEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') ELSE GET_JSON_OBJECT(MAX(extra_info), '$.src') END AS source FROM sng_tdbank . cc_dsl_content_center_rpt_fdt0 WHERE tdbank_imp_date BETWEEN DATE_SUB(20190226, 90) AND 20190226 AND op_type = '0XCC0V000' AND GET_JSON_OBJECT(extra_info, '$.renewal') NOT IN ('1') AND src IN ( '2' ,'5' ,'6' ,'10' ,'12' ,'15' ) GROUP BY puin ,row_key ) D ON C.row_key = D.row_key";
		String sql54_a = "INSERT INTO TABLE t_kandian_account_video_uv_daily_new SELECT 20190226 ,C.puin ,C.row_key ,CASE WHEN source IN( '1' ,'3') THEN 1 ELSE 0 END AS is_kd_source ,uv ,vv FROM ( SELECT puin ,A.row_key ,COUNT(DISTINCT A.cuin) AS uv ,SUM(A.vv) AS vv FROM ( SELECT cuin ,business_id AS puin ,op_cnt AS vv ,rowkey AS row_key ,RANK() OVER ( PARTITION BY rowkey ORDER BY ftime ) AS f_rank FROM v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate = 20190226 AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ( 'ab' ,'ae' ,'af' ,'aj' ,'al' ,'ao' ) AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 ) A LEFT JOIN ( SELECT MAX(fdate) AS tdbank_imp_date ,rowkey AS row_key ,SUM(op_cnt) AS history_vv FROM v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate BETWEEN DATE_SUB(20190226, 90) AND DATE_SUB(20190226, 1) AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ( 'ab' ,'ae' ,'af' ,'aj' ,'al' ,'ao' ) AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 GROUP BY rowkey ) B ON A.row_key = B.row_key WHERE ( ( B.history_vv IS NOT NULL AND f_rank < (3000001 - B.history_vv) ) OR ( f_rank < 3000001 AND B.history_vv IS NULL ) ) GROUP BY A.puin ,A.row_key ) C LEFT JOIN ( SELECT puin ,row_key ,CASE WHEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') IS NOT null THEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') ELSE GET_JSON_OBJECT(MAX(extra_info), '$.src') END AS source FROM cc_dsl_content_center_rpt_fdt0 WHERE tdbank_imp_date BETWEEN DATE_SUB(20190226, 90) AND 20190226 AND op_type = '0XCC0V000' AND GET_JSON_OBJECT(extra_info, '$.renewal') NOT IN ('1') AND src IN ( '2' ,'5' ,'6' ,'10' ,'12' ,'15' ) GROUP BY puin ,row_key ) D ON C.row_key = D.row_key";
		String sql61 = "INSERT INTO TABLE t_kandian_account_video_uv_daily_new SELECT 20190226, 'aaaaa' AS s_a, C.puin puin , C.row_key , CASE WHEN SOURCE IN('1' ,'3') THEN 1 ELSE 0 END AS is_kd_source , CASE WHEN SOURCE='hello' THEN 1 ELSE 0 END AS s_kd_source , uv, vv a_vv, c.uv c_uv, d.puin d_puin FROM(SELECT puin , A.row_key , COUNT(DISTINCT A.cuin) AS uv , SUM(A.vv) AS vv FROM (SELECT cuin , business_id AS puin , op_cnt AS vv , rowkey AS row_key , RANK() OVER (PARTITION BY rowkey ORDER BY ftime) AS f_rank FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate = 20190226 AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ('ab' , 'ae' , 'af' , 'aj' , 'al' , 'ao') AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100) A LEFT JOIN (SELECT MAX(fdate) AS tdbank_imp_date , rowkey AS row_key , SUM(op_cnt) AS history_vv FROM sng_cp_fact.v_ty_BBBB WHERE fdate BETWEEN DATE_SUB(20190226, 90) AND DATE_SUB(20190226, 1) AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ('ab' , 'ae' , 'af' , 'aj' , 'al' , 'ao') AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 GROUP BY rowkey) B ON A.row_key = B.row_key WHERE ((B.history_vv IS NOT NULL AND f_rank < (3000001 - B.history_vv)) OR (f_rank < 3000001 AND B.history_vv IS NULL)) GROUP BY A.puin , A.row_key) C LEFT JOIN (SELECT puin , row_key , CASE WHEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') IS NOT NULL THEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') ELSE GET_JSON_OBJECT(MAX(extra_info), '$.src') END AS SOURCE FROM sng_tdbank . cc_dsl_content_center_rpt_fdt0 WHERE tdbank_imp_date BETWEEN DATE_SUB(20190226, 90) AND 20190226 AND op_type = '0XCC0V000' AND GET_JSON_OBJECT(extra_info, '$.renewal') NOT IN ('1') AND src IN ('2' , '5' , '6' , '10' , '12' , '15') GROUP BY puin , row_key) D ON C.row_key = D.row_key";
		String parsesql = sql61;

		HiveTableLineageParserBriefTemp2 hp = new HiveTableLineageParserBriefTemp2();
		System.out.println(parsesql);
		ASTNode ast = null;
		try {
			ast = pd.parse(parsesql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(ast.toStringTree());
		JamesUtil.printDivider();
		hp.parse(ast);

		JamesUtil.printDivider();
		System.out.println(tgtTable.getTableName());
		for (TableNode t : srcTables) {
			System.out.println(t.getTableName());
		}

		TableRelation tableRelation = new TableRelation(srcTables, tgtTable);
		System.out.println(tableRelation);

		JamesUtil.printDivider();
		System.out.println("insertFieldMap...");
		JamesUtil.printStringMap(insertSelectFieldMap);

		JamesUtil.printDivider();
		JamesUtil.printStack(tokDbNameStack);
		JamesUtil.printDivider();
		System.out.println("tokTableNameStack...");
		JamesUtil.printStack(tokTableNameStack);
		JamesUtil.printDivider();
		for (Entry<String, Set<String>> set : tableAliasSetMap.entrySet()) {
			System.out.println(set.getKey() + " -> ");
			for (String s : set.getValue()) {
				System.out.print("\t" + s);
			}
			System.out.println();
		}
	}
}