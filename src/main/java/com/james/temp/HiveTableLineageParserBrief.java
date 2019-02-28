package com.james.temp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import com.james.common.util.JamesUtil;

/**
 * 目的：获取AST中的表，列，以及对其所做的操作，如SELECT,INSERT 重点：获取SELECT操作中的表和列的相关操作。其他操作这判断到表级别。
 * 实现思路：对AST深度优先遍历，遇到操作的token则判断当前的操作，
 * 遇到TOK_TAB或TOK_TABREF则判断出当前操作的表，遇到子句则压栈当前处理，处理子句。 子句处理完，栈弹出。
 *
 */
public class HiveTableLineageParserBrief {
    private static String currentTableName = "";
    private static List<TableNode> srcTables = new ArrayList<TableNode>();
    private static TableNode tgtTable = new TableNode();

    private static final String UNKNOWN = "UNKNOWN";

    private static Map<String, String> cols = new TreeMap<String, String>();
    private static Map<String, String> tableAliasMap = new HashMap<String, String>();
    private static Map<String, String> fieldAliasMap = new TreeMap<String, String>();
    private static Map<String, String> tgtFieldMap = new TreeMap<String, String>();

    /**
     * tables存入的是每个表名以及表名对应的操作 String = tableName + "\t" + oper
     */
    private Set<String> tables = new HashSet<String>();

    private Stack<String> tableNameStack = new Stack<String>();
    private Stack<Oper> operStack = new Stack<Oper>();
    private String nowQueryTable = "";// 定义及处理不清晰，修改为query或from节点对应的table集合或许好点。目前正在查询处理的表可能不止一个。
    private Oper oper;
    private boolean joinClause = false;

    private enum Oper {
        SELECT, INSERT, TRUNCATE, LOAD, CREATE＿TABLE, ALTER, DROP_TABLE, SHOW, DELETE, UPDATE, DESC
    }

    public Set<String> parseIteral(ASTNode ast) {// 属于深度解析
        // System.out.println(oper);
        Set<String> set = new HashSet<String>();// 当前查询所对应到的表集合
        prepareToParseCurrentNodeAndChilds(ast);
        set.addAll(parseChildNodes(ast));
        set.addAll(parseCurrentNode(ast, set));
        endParseCurrentNode(ast);
        return set;
    }

    private void endParseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {// join 从句结束，跳出join
            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
                joinClause = false;
                break;
            case HiveParser.TOK_QUERY:
                break;
            case HiveParser.TOK_INSERT:
            case HiveParser.TOK_SELECT:
                nowQueryTable = tableNameStack.pop();
                oper = operStack.pop();
                break;
            }
        }
    }

    /* parseCurrentNode */
    private Set<String> parseCurrentNode(ASTNode ast, Set<String> set) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
            case HiveParser.TOK_TABLE_PARTITION:
                // case HiveParser.TOK_TABNAME:
                if (ast.getChildCount() != 2) {
                    String table = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                    if (oper == Oper.SELECT) {
                        nowQueryTable = table;
                    }
                    // tables.add(table + " <- " + oper);
                }
                break;

            case HiveParser.TOK_TAB:// outputTable
                String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                if (oper == Oper.SELECT) {
                    nowQueryTable = tableTab;
                }
                tables.add(tableTab + "\t" + oper);
                tgtTable.setTableName(tableTab);
                break;
            case HiveParser.TOK_DELETE_FROM:// outputTable
                String deletetab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                if (oper == Oper.DELETE) {
                    nowQueryTable = deletetab;
                }
                tables.add(deletetab + "\t" + oper);
                break;
            case HiveParser.TOK_UPDATE_TABLE:// outputTable
                String updatetab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                if (oper == Oper.UPDATE) {
                    nowQueryTable = updatetab;
                }
                tables.add(updatetab + "\t" + oper);
                break;
            case HiveParser.TOK_TABREF:// inputTable
                currentTableName = ast.getChild(0).getChild(0).getText().toLowerCase();
                System.out.println("currentTableName: " + currentTableName);

                ASTNode tabTree = (ASTNode) ast.getChild(0);
                String tableName = (tabTree.getChildCount() == 1)
                        ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                        : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "."
                                + tabTree.getChild(1);
                if (oper == Oper.SELECT) {
                    if (joinClause && !"".equals(nowQueryTable)) {
                        nowQueryTable += "&" + tableName;//
                    } else {
                        nowQueryTable = tableName;
                    }
                    set.add(tableName);
                }
                tables.add(tableName + "\t" + oper);
                srcTables.add(new TableNode(tableName));
                if (ast.getChild(1) != null) {
                    String alia = ast.getChild(1).getText().toLowerCase();
                    tableAliasMap.put(alia, tableName);// sql6 p别名在tabref只对应为一个表的别名。
                }
                break;
            case HiveParser.TOK_TABLE_OR_COL:
                if (ast.getParent().getType() != HiveParser.DOT) {
                    String col = ast.getChild(0).getText().toLowerCase();
                    if (tableAliasMap.get(col) == null && fieldAliasMap.get(nowQueryTable + "." + col) == null) {
                        if (nowQueryTable.indexOf("&") > 0) {// sql23
                            cols.put(UNKNOWN + "." + col, "");
                        } else {
                            cols.put(nowQueryTable + "." + col, "");
                        }
                    }
                }
                break;
            case HiveParser.TOK_ALLCOLREF:
                if (ast.getChildCount() > 0) {
                    ASTNode tree = (ASTNode) ast.getChild(0);
                    String selectTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tree);
                    if (tableAliasMap.get(selectTable) != null) {
                        selectTable = tableAliasMap.get(selectTable);
                    }
                    cols.put(selectTable + ".*", "");
                } else {
                    cols.put(nowQueryTable + ".*", "");
                }
                break;
            case HiveParser.TOK_FUNCTIONSTAR:
                cols.put(nowQueryTable + ".*", "");
                break;
            case HiveParser.TOK_SUBQUERY:
                if (ast.getChildCount() == 2) {
                    String tableAlias = unescapeIdentifier(ast.getChild(1).getText());
                    String aliaReal = "";
                    for (String table : set) {
                        aliaReal += table + "&";
                    }
                    if (aliaReal.length() != 0) {
                        aliaReal = aliaReal.substring(0, aliaReal.length() - 1);
                    }
                    // alias.put(tableAlias, nowQueryTable);//sql22
                    tableAliasMap.put(tableAlias, aliaReal);// sql6
                    // alias.put(tableAlias, "");// just store alias
                }
                break;

            case HiveParser.TOK_SELEXPR:
                String fieldName = "";
                String aliasFieldName = "";

                if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
                    fieldName = ast.getChild(0).getChild(0).getText().toLowerCase();
                    aliasFieldName = null == ast.getChild(1) ? fieldName : ast.getChild(1).getText().toLowerCase();

                    System.out.println("字段別名: " + currentTableName + "." + aliasFieldName + " -> " + fieldName);
                    fieldAliasMap.put(currentTableName + "." + aliasFieldName, currentTableName + "." + fieldName);
                } else if (ast.getChild(0).getType() == HiveParser.TOK_FUNCTION) {
                    if (ast.getChild(0).getChild(1).getType() == HiveParser.TOK_TABLE_OR_COL) {
                        fieldName = ast.getChild(0).getChild(0).getText() + "("
                                + ast.getChild(0).getChild(1).getChild(0).getText() + ")";
                        aliasFieldName = null == ast.getChild(1) ? fieldName : ast.getChild(1).getText().toLowerCase();

                        System.out.println("字段別名: " + currentTableName + "." + aliasFieldName + " -> " + fieldName);
                        fieldAliasMap.put(currentTableName + "." + aliasFieldName, currentTableName + "." + fieldName);

                    } else if (ast.getChild(0).getChild(1).getType() == HiveParser.DOT) {
                        String tgtTableName = ast.getChild(0).getChild(1).getChild(0).getChild(0).getText()
                                .toLowerCase();
                        String tgtFieldName = ast.getChild(0).getChild(0).getText() + "("
                                + ast.getChild(0).getChild(1).getChild(1).getText() + ")";
                        // System.out.println("tgtTableName=" + tgtTableName + " && " + "tgtFieldName="
                        // + tgtFieldName);

                        String tgtAliasFieldName = ast.getChild(1) == null ? tgtFieldName
                                : ast.getChild(1).getText().toLowerCase();

                        System.out
                                .println("--> 字段选择: " + tgtAliasFieldName + " -> " + tgtTableName + "." + tgtFieldName);
                        tgtFieldMap.put(tgtAliasFieldName, tgtTableName + "." + tgtFieldName);
                    }

                } else if (ast.getChild(0).getType() == HiveParser.DOT) {
                    if (ast.getChild(0).getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
                        String tgtTableName = ast.getChild(0).getChild(0).getChild(0).getText().toLowerCase();
                        String tgtFieldName = ast.getChild(0).getChild(1).getText().toLowerCase();
                        // System.out.println("tgtTableName=" + tgtTableName + " && " + "tgtFieldName="
                        // + tgtFieldName);

                        String tgtAliasFieldName = ast.getChild(1) == null ? tgtFieldName
                                : ast.getChild(1).getText().toLowerCase();

                        System.out
                                .println("--> 字段选择: " + tgtAliasFieldName + " -> " + tgtTableName + "." + tgtFieldName);
                        tgtFieldMap.put(tgtAliasFieldName, tgtTableName + "." + tgtFieldName);
                    }
                }
                break;

            case HiveParser.DOT:
                if (ast.getType() == HiveParser.DOT) {
                    if (ast.getChildCount() == 2) {
                        if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                                && ast.getChild(0).getChildCount() == 1
                                && ast.getChild(1).getType() == HiveParser.Identifier) {
                            String alia = BaseSemanticAnalyzer
                                    .unescapeIdentifier(ast.getChild(0).getChild(0).getText().toLowerCase());
                            String column = BaseSemanticAnalyzer
                                    .unescapeIdentifier(ast.getChild(1).getText().toLowerCase());
                            String realTable = null;
                            if (!tables.contains(alia + "\t" + oper) && tableAliasMap.get(alia) == null) {// [b
                                // SELECT,
                                // a
                                // SELECT]
                                tableAliasMap.put(alia, nowQueryTable);
                            }
                            if (tables.contains(alia + "\t" + oper)) {
                                realTable = alia;
                            } else if (tableAliasMap.get(alia) != null) {
                                realTable = tableAliasMap.get(alia);
                            }
                            if (realTable == null || realTable.length() == 0 || realTable.indexOf("&") > 0) {
                                realTable = UNKNOWN;
                            }
                            cols.put(realTable + "." + column, "");

                        }
                    }
                }
                break;

            case HiveParser.TOK_ALTERTABLE_ADDPARTS:
            case HiveParser.TOK_ALTERTABLE_RENAME:
            case HiveParser.TOK_ALTERTABLE_ADDCOLS:

                // ASTNode alterTableName = (ASTNode) ast.getChild(0);
                // tables.add(alterTableName.getText() + "\t" + oper);
            case HiveParser.TOK_ALTERTABLE:
                ASTNode alterNode = (ASTNode) ast.getChild(0);
                if (alterNode.getToken().getType() == HiveParser.TOK_TABNAME) {
                    String alterTableName = BaseSemanticAnalyzer.getUnescapedName(alterNode);
                    tables.add(alterTableName + "\t" + oper);
                }
                break;
            case HiveParser.TOK_CREATETABLE:
                oper = Oper.CREATE＿TABLE;
                ASTNode createNode = (ASTNode) ast.getChild(0);
                String createTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) createNode);
                tables.add(createTableName + "\t" + oper);
                break;
            case HiveParser.TOK_DROPTABLE:
                oper = Oper.DROP_TABLE;
                ASTNode dropNode = (ASTNode) ast.getChild(0);
                String dropTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) dropNode);
                tables.add(dropTableName + "\t" + oper);
                break;
            }
        }

        if (ast.getToken() != null && ast.getToken().getType() >= HiveParser.TOK_SHOWCOLUMNS
                && ast.getToken().getType() <= HiveParser.TOK_SHOW_TBLPROPERTIES) {
            ASTNode dropNode = (ASTNode) ast.getChild(0);
            String dropTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) dropNode);
            tables.add(dropTableName + "\t" + oper);
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
            tables.add(descTableName + "\t" + oper);
        }

        return set;
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
     * 准备去解析当前的节点和其子节点
     * 
     * @param ast
     */
    private void prepareToParseCurrentNodeAndChilds(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {// join 从句开始
            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
                joinClause = true;
                break;
            case HiveParser.TOK_QUERY:
                tableNameStack.push(nowQueryTable);
                operStack.push(oper);
                nowQueryTable = "";// sql22
                oper = Oper.SELECT;
                break;
            case HiveParser.TOK_INSERT:
                tableNameStack.push(nowQueryTable);
                operStack.push(oper);
                oper = Oper.INSERT;
                break;
            case HiveParser.TOK_DELETE_FROM:
                tableNameStack.push(nowQueryTable);
                operStack.push(oper);
                oper = Oper.DELETE;
                nowQueryTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));// sql22
                break;
            case HiveParser.TOK_UPDATE_TABLE:
                tableNameStack.push(nowQueryTable);
                operStack.push(oper);
                oper = Oper.UPDATE;
                nowQueryTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));// sql22
                break;
            case HiveParser.TOK_SELECT:
                tableNameStack.push(nowQueryTable);
                operStack.push(oper);
                // nowQueryTable = nowQueryTable
                // nowQueryTable = "";//语法树join
                // 注释语法树sql9， 语法树join对应的设置为""的注释逻辑不符
                oper = Oper.SELECT;
                break;
            case HiveParser.TOK_DROPTABLE:
                tableNameStack.push(nowQueryTable);
                operStack.push(oper);
                oper = Oper.DROP_TABLE;
                break;
            case HiveParser.TOK_TRUNCATETABLE://
                oper = Oper.TRUNCATE;
                break;
            case HiveParser.TOK_LOAD:
                oper = Oper.LOAD;
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

        System.out.println("***************目标字段***************");
        output(tgtFieldMap);
        System.out.println("***************表别名***************");
        output(tableAliasMap);
        System.out.println("***************字段别名***************");
        output(fieldAliasMap);
        System.out.println("***************表***************");
        for (String table : tables) {
            System.out.println(table);
        }
        // System.out.println("***************列***************");
        // output(cols);

    }

    public static void main(String[] args) {
        ParseDriver pd = new ParseDriver();
        String sql52 = "INSERT INTO TABLE t_target \r\n"
                + "SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_a.same FROM (\r\n"
                + "SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a \r\n"
                + "LEFT JOIN (\r\n"
                + "SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key";

        String sql52_a = "SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc";

        String sql52_b = "INSERT INTO TABLE t_target \r\n"
                + "SELECT MAX(r_t_a.r_a_a) AS f_a_a, r_t_b.r_b_b AS f_b_b, MAX(r_t_a.r_a_c) AS f_a_c\r\n" + "FROM (\r\n"
                + "	SELECT a_key, a_a AS r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c\r\n" + "		, MAX(same) AS same\r\n"
                + "	FROM t_a\r\n" + "	WHERE a_a = 1\r\n" + "	GROUP BY a_key, a_a, a_b\r\n"
                + "	ORDER BY a_a DESC\r\n" + ") r_t_a\r\n" + "	LEFT JOIN (\r\n"
                + "		SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b\r\n"
                + "			, MAX(b_c) AS r_b_c, MAX(same) AS same\r\n" + "		FROM t_b\r\n"
                + "		GROUP BY b_key\r\n" + "		ORDER BY b_b\r\n" + "	) r_t_b\r\n"
                + "	ON r_t_a.a_key = r_t_b.b_key";

        String sql53 = "insert into t_all_video_play_basic_info  "
                + "SELECT SUBSTR(tdbank_imp_date, 1, 8) AS fdate ,\r\n"
                + "                       reporttime AS ftime ,\r\n" + "                       CASE\r\n"
                + "                           WHEN op_type IN ( '0X8007408' ,'0X8007409' ) THEN d3\r\n"
                + "                           ELSE GET_JSON_OBJECT(d4, '$.rowkey')\r\n"
                + "                       END AS rowkey ,\r\n" + "                       CASE\r\n"
                + "                           WHEN op_type IN ( '0X8007408' ,'0X8007409' ) THEN d3\r\n"
                + "                           ELSE GET_JSON_OBJECT(d4, '$.rowkey')\r\n"
                + "                       END AS vid ,\r\n" + "                       '' as bid ,\r\n"
                + "                       COALESCE(touin, TO_NUMBER(GET_JSON_OBJECT(d4, '$.puin'))) AS business_id ,\r\n"
                + "                       '' AS title ,\r\n" + "                       '' AS category ,\r\n"
                + "                       '' AS sub_category ,\r\n"
                + "                       GET_JSON_OBJECT(d4, '$.video_duration') AS video_length ,\r\n"
                + "                       1 AS is_original ,\r\n" + "                       1 AS is_exclusive ,\r\n"
                + "                       1 AS is_short_video ,\r\n" + "                       1 AS is_horizontal ,\r\n"
                + "                       '1' AS dis_platform ,\r\n" + "                       uin AS cuin ,\r\n"
                + "                       uin AS login_id ,\r\n" + "                       CASE\r\n"
                + "                           WHEN b.platform_name = 'Android' THEN imei\r\n"
                + "                       END AS imei ,\r\n" + "                       CASE\r\n"
                + "                           WHEN b.platform_name = 'iOS' THEN imei\r\n"
                + "                       END AS idfa ,\r\n" + "                       '' AS idfv ,\r\n"
                + "                       GET_JSON_OBJECT(d4, '$.imsi') AS imsi ,\r\n"
                + "                       '' AS androidid ,\r\n" + "                       ip ,\r\n"
                + "                       b.platform_name AS platform ,\r\n"
                + "                       '' AS mobile_type ,\r\n" + "                       '' as mac ,\r\n"
                + "                       GET_JSON_OBJECT(d4, '$.network_type') as network_type ,\r\n"
                + "                       '' AS wifi_ssid ,\r\n" + "                       '' AS wifi_mac ,\r\n"
                + "                       b.version_name AS app_version ,\r\n"
                + "                       b.sub_version AS app_subversion ,\r\n"
                + "                       3 op_type ,\r\n" + "                       op_cnt ,\r\n"
                + "                       GET_JSON_OBJECT(d4, '$.watch_duration') AS play_time ,\r\n"
                + "                       GET_JSON_OBJECT(d4, '$.current_duration') AS current_time ,\r\n"
                + "                       '' as channel ,\r\n" + "                       '' as soucre ,\r\n"
                + "                       '' as longitude ,\r\n" + "                       '' as latitude ,\r\n"
                + "                       '' as extra_info1 ,\r\n" + "                       '' as extra_info2 ,\r\n"
                + "                       '' as extra_info3 ,\r\n" + "                       '' as extra_info4 ,\r\n"
                + "                       '' as extra_info5\r\n" + "FROM hlw.t_dw_dc01160 a\r\n"
                + "LEFT OUTER JOIN imdataoss.im_data_01_05_001_daily PARTITION (p_20190115) b ON a.appid = b.lc_appid\r\n"
                + "WHERE SUBSTR(tdbank_imp_date, 1, 8) = 20180115\r\n"
                + "  AND op_type IN ( '0X8007408' ,'0X8007409' )";
        
        String sql54="INSERT INTO TABLE t_kandian_account_video_uv_daily_new SELECT 20190226 ,C.puin ,C.row_key ,CASE WHEN source IN( '1' ,'3') THEN 1 ELSE 0 END AS is_kd_source ,uv ,vv FROM ( SELECT puin ,A.row_key ,COUNT(DISTINCT A.cuin) AS uv ,SUM(A.vv) AS vv FROM ( SELECT cuin ,business_id AS puin ,op_cnt AS vv ,rowkey AS row_key ,RANK() OVER ( PARTITION BY rowkey ORDER BY ftime ) AS f_rank FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate = 20190226 AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ( 'ab' ,'ae' ,'af' ,'aj' ,'al' ,'ao' ) AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 ) A LEFT JOIN ( SELECT MAX(fdate) AS tdbank_imp_date ,rowkey AS row_key ,SUM(op_cnt) AS history_vv FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate BETWEEN DATE_SUB(20190226, 90) AND DATE_SUB(20190226, 1) AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ( 'ab' ,'ae' ,'af' ,'aj' ,'al' ,'ao' ) AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 GROUP BY rowkey ) B ON A.row_key = B.row_key WHERE ( ( B.history_vv IS NOT NULL AND f_rank < (3000001 - B.history_vv) ) OR ( f_rank < 3000001 AND B.history_vv IS NULL ) ) GROUP BY A.puin ,A.row_key ) C LEFT JOIN ( SELECT puin ,row_key ,CASE WHEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') IS NOT null THEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') ELSE GET_JSON_OBJECT(MAX(extra_info), '$.src') END AS source FROM sng_tdbank . cc_dsl_content_center_rpt_fdt0 WHERE tdbank_imp_date BETWEEN DATE_SUB(20190226, 90) AND 20190226 AND op_type = '0XCC0V000' AND GET_JSON_OBJECT(extra_info, '$.renewal') NOT IN ('1') AND src IN ( '2' ,'5' ,'6' ,'10' ,'12' ,'15' ) GROUP BY puin ,row_key ) D ON C.row_key = D.row_key";

        // String parsesql = sql52_a;
        // String parsesql = sql52;
        String parsesql = sql52_b;
        HiveTableLineageParserBrief hp = new HiveTableLineageParserBrief();
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
        // System.out.println(hp.oper);

        JamesUtil.printDivider();
        // System.out.println(tgtTable.getTableName());
        // for (TableNode t : srcTables) {
        // System.out.println(t.getTableName());
        // }

        TableRelation tableRelation = new TableRelation(srcTables, tgtTable);
        System.out.println(tableRelation);

        JamesUtil.printDivider();
        Set<String> setKey = tgtFieldMap.keySet();
        for (String key : setKey) {
            String value = tgtFieldMap.get(key);
            System.out.println("\tkey: " + key + " --> " + "value: " + value);

            String tableName = SqlFunctionUtil.getTableName(value);
            String tableData = SqlFunctionUtil.getTableData(value);
            String originTableName = tableAliasMap.get(tableName);
            System.out.println(tableName + " --> " + originTableName);
            value = originTableName + "." + SqlFunctionUtil.removeSqlFunctionName(tableData);

            System.out.println(value + " --> " + SqlFunctionUtil.removeSqlFunctionName(fieldAliasMap.get(value)));
            System.out.println(" * " + key + " --> " + SqlFunctionUtil.removeSqlFunctionName(fieldAliasMap.get(value)));
            System.out.println();
        }
    }
}