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
public class HiveTableLineageParser {
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
        // HiveConf conf = new HiveConf();
        String sql1 = "Select * from zpc1";
        String sql2 = "Select name,ip from zpc2 bieming where age > 10 and area in (select area from city)";
        String sql3 = "Select d.name,d.ip from (select * from zpc3 where age > 10 and area in (select area from city)) d";
        String sql4 = "create table zpc(id string, name string)";
        String sql5 = "insert overwrite table tmp1 PARTITION (partitionkey='2008-08-15') select * from tmp";
        String sql6 = "FROM (  SELECT p.datekey datekey, p.userid userid, c.clienttype  FROM detail.usersequence_client c JOIN fact.orderpayment p ON p.orderid = c.orderid "
                + " JOIN default.user du ON du.userid = p.userid WHERE p.datekey = 20131118 ) base  INSERT OVERWRITE TABLE `test`.`customer_kpi` SELECT base.datekey, "
                + "  base.clienttype, count(distinct base.userid) buyer_count GROUP BY base.datekey, base.clienttype";
        String sql7 = "SELECT id, value FROM (SELECT id, value FROM p1 UNION ALL  SELECT 4 AS id, 5 AS value FROM p1 limit 1) u";
        String sql8 = "select dd from(select id+1 dd from zpc) d";
        String sql9 = "select dd+1 from(select id+1 dd from zpc) d";
        String sql10 = "truncate table zpc";
        String sql11 = "drop table zpc";
        String sql12 = "select * from tablename where unix_timestamp(cz_time) > unix_timestamp('2050-12-31 15:32:28')";
        String sql15 = "alter table old_table_name RENAME TO new_table_name";
        String sql16 = "select statis_date,time_interval,gds_cd,gds_nm,sale_cnt,discount_amt,discount_rate,price,etl_time,pay_amt from o2ostore.tdm_gds_monitor_rt where time_interval = from_unixtime(unix_timestamp(concat(regexp_replace(from_unixtime(unix_timestamp('201506181700', 'yyyyMMddHHmm')+ 84600 ,  'yyyy-MM-dd HH:mm'),'-| |:',''),'00'),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss')";
        String sql13 = "INSERT OVERWRITE TABLE u_data_new SELECT TRANSFORM (userid, movieid, rating, unixtime) USING 'python weekday_mapper.py' AS (userid, movieid, rating, weekday) FROM u_data";
        String sql14 = "SELECT a.* FROM a JOIN b ON (a.id = b.id AND a.department = b.department)";
        String sql17 = "LOAD DATA LOCAL INPATH \"/opt/data/1.txt\" OVERWRITE INTO TABLE table1";
        String sql18 = "CREATE TABLE  table1     (    column1 STRING COMMENT 'comment1',    column2 INT COMMENT 'comment2'        )";
        String sql19 = "ALTER TABLE events RENAME TO 3koobecaf";
        String sql20 = "ALTER TABLE invites ADD COLUMNS (new_col2 INT COMMENT 'a comment')";
        String sql21 = "alter table mp add partition (b='1', c='1')";
        String sql22 = "select login.uid from login day_login left outer join (select uid from regusers where dt='20130101') day_regusers on day_login.uid=day_regusers.uid where day_login.dt='20130101' and day_regusers.uid is null";
        String sql23 = "select name from (select * from zpc left outer join def) d";
        String sql24 = "  select t1.*,t2.value_data  from t_hm_ru_03 t1"
                + " join ( select * from s_base_values where pt = '20110410000000' and value_id = 888 ) t2 "
                + " on t1.brand_id = t2.value_id";

        String sql25 = " Select star_name as name, count(1) from users u where pt = '20110325000000' and user_gender = 1 group by star_name";
        String sql26 = "drop table fff";
        // TOK_TABNAME users TOK_CREATETABLE(create table) TOK_DROPTABLE(drop
        // tableＺ )
        // TOK_TABNAME users TOK_CREATETABLE(create table) TOK_DROPTABLE(drop
        // tableＺ )
        String sql27 = "create table student(age int,name string)";

        String sql28 = "select age,name from s.student stu ";// left join person
                                                             // p on stu.userid
                                                             // = p.id

        String sql29 = "update student set name=\"yanggang\" where userId=2";

        String sql30 = "delete from student where userid=2";

        String sql31 = "select * from student";

        String sql32 = "select t.* from logs.service_log_nh s left join test t on s.id=t.id ";

        String sql33 = "select a.*,b.age,c.name from aa a left join bb b on a.id = b.id left join cc c on c.id = a.id";

        String sql34 = "create table test.sudent3333fff(a int,b int,c int)";

        String sql35 = "drop table student";

        String sql36 = " create table logs.testCreateTable as select url,method,ts from logs.service_log_nh limit 10";

        String sql37 = "insert into table student(a,b) values(1,2)";

        String sql38 = "select * from logs.2ee2test";

        String sql39 = "show create table student"; // TOK_SHOW_CREATETABLE

        String sql40 = "show partitions edw_public.dim_esf_edw_pub_geography";

        String sql41 = "DESCRIBE 22test";

        String sql42 = "DESCRIBE database logs";

        String sql43 = "set dt ='20161017'";

        String sql44 = "SET hive.auto.convert.join=false";

        String sql45 = "select a1.age,a1.sex from (select s.age,s.id,s.sex from student as s) as a1 ";

        String sql46 = "select sum(*) from fangdd_data.user_trace where dt=20161002";

        String sql47 = "SELECT date(a.actionTime),count(DISTINCT a.gaId) uv from fangdd_data.user_trace_lzo as a  where a.dt>='20161122' and a.dt<='20161122' and a.sourceid=3  and a.event='头条-详情-下方-分享' GROUP BY date(a.actionTime)";

        String sql48 = "ALTER TABLE  tmp.student ADD COLUMNS (e int)";

        String sql49 = "drop table if exists tmp.yuan_intermid20161201";

        String sql50 = "INSERT OVERWRITE TABLE arp_nav_edge_tmp " + "SELECT "
                + "\" NAV_EDGE_IMPRESSION\" as event_name,"
                + "concat('2017',lpad('05',2,'0'),lpad('31',2,'0')) as date_id,"
                + "CONCAT(payload.log_context.log_id, '_', trv_pos) as log_id," + "payload.log_context.reg_vid,"
                + "payload.log_context.visitor_id," + "payload.log_context.carrier," + "payload.log_context.app_id "
                + "FROM arp_client_events_stg "
                + "LATERAL VIEW posexplode(payload.travelled_edge_id_list) trv_explode as trv_pos"
                + " WHERE payload.event_name = 'NAV_EDGES'";

        String sql51 = "INSERT OVERWRITE TABLE unified_client_events_0_flattened SELECT * FROM ( SELECT a1,a2,a3 FROM unified_client_events_0 where a1 in ('USER_ACTION','SDCARD_CHANGE') UNION ALL SELECT b1,b2,b3 FROM unified_client_events_1 where b1 in ('CARD_INTERACTION')) stg_temp";

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

        // String parsesql = sql52_a;
        // String parsesql = sql52;
        String parsesql = sql52_b;
        HiveTableLineageParser hp = new HiveTableLineageParser();
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