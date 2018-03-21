package com.james.hive.parser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import com.james.common.util.JamesUtil;

public class HiveParser1 {
    private static Set<String> opers = new HashSet<String>();

    private static enum OPERATION_TOKON {
        SELECT, INSERT, DROP, TRUNCATE, LOAD, CREATETABLE, ALTER, SHOW, DESC, DELETE, UPDATE
    }

    private static final String UNKNOWN = "UNKNOWN";

    private static String excute_script;
    private static Set<String> tables = new HashSet<String>();
    private static Map<String, String> tableAlias = new HashMap<String, String>();
    private static Set<String> cols = new HashSet<String>();
    private static Map<String, String> colAlais = new HashMap<String, String>();
    private static Stack<OPERATION_TOKON> operStack = new Stack<OPERATION_TOKON>();

    private static OPERATION_TOKON oper;
    private static boolean joinClause = false;

    private static Stack<String> tableNameStack = new Stack<String>();

    private static String nowQueryTable = "";

    private static boolean operMetaInfo = false;

    private static boolean isSelectOper = true;

    private static boolean isShowOper = false;

    private static boolean isDescOper = false;

    static {
        // param
        opers.add("; set");

        // DDL
        opers.add("; create");
        opers.add("; drop");
        opers.add("; alert");
        opers.add("; truncate");
        opers.add("; show");
        opers.add("; describe");

        // DML
        opers.add("; insert");
        opers.add("; load");
        opers.add("; update");
        opers.add("; delete");
        opers.add("; import");
        opers.add("; export");
        opers.add("; explain");

        // DQL
        opers.add("; select");
    }

    public static void main(String[] args) throws ParseException {
        String hiveSql = null;
        ASTNode ast = null;
        ParseDriver pd = new ParseDriver();

        // hiveSql = "select * from mydb.u";
        // ast = pd.parse(hiveSql);
        // System.out.println(ast.toStringTree());
        // JamesUtil.printDivider();

        /**
         * INSERT OVERWRITE TABLE arp_nav_edge_tmp SELECT "NAV_EDGE_IMPRESSION" as
         * event_name,
         * concat(${log_year},lpad(${log_month},2,'0'),lpad(${log_day},2,'0')) as
         * date_id, CONCAT(payload.log_context.log_id, '_', trv_pos) as log_id,
         * payload.log_context.reg_vid, payload.log_context.visitor_id,
         * payload.log_context.carrier, payload.log_context.app_id FROM
         * arp_client_events_stg LATERAL VIEW posexplode(payload.travelled_edge_id_list)
         * trv_explode as trv_pos,trv WHERE payload.event_name = 'NAV_EDGES';
         */
        hiveSql = "INSERT OVERWRITE TABLE arp_nav_edge_tmp " + "SELECT " + "\" NAV_EDGE_IMPRESSION\" as event_name,"
                + "concat('2017',lpad('05',2,'0'),lpad('31',2,'0')) as date_id,"
                + "CONCAT(payload.log_context.log_id, '_', trv_pos) as log_id," + "payload.log_context.reg_vid,"
                + "payload.log_context.visitor_id," + "payload.log_context.carrier," + "payload.log_context.app_id "
                + "FROM arp_client_events_stg "
                + "LATERAL VIEW posexplode(payload.travelled_edge_id_list) trv_explode as trv_pos"
                + " WHERE payload.event_name = 'NAV_EDGES'";

        System.out.println();
        System.out.println("hiveSql=\n" + hiveSql);
        hiveSql = toLowerCaseAndFormat(hiveSql);
        System.out.println();
        System.out.println("hiveSql=\n" + hiveSql);

        ast = pd.parse(hiveSql);
        System.out.println("\npd.parse(hiveSql)--------");
        System.out.println(ast.toStringTree());
        System.out.println();

        int nChildCount = ast.getChildCount();
        System.out.println("nChildCount=" + nChildCount);
        System.out.println();

        System.out.println("\nast.getChild(i)--------");
        for (int i = 0; i < nChildCount; i++) {
            Tree child = ast.getChild(i);
            System.out.println("\n\tchild: " + i);
            System.out.println(child.toStringTree());

            int nGrandChildCount = child.getChildCount();
            for (int j = 0; j < nGrandChildCount; j++) {
                Tree grandChild = child.getChild(j);
                System.out.println("\n\t\tgrandChild: " + j);
                System.out.println(grandChild.toStringTree());
            }
        }

        JamesUtil.printDivider();

        // parse(hiveSql);
        // for (String str : getCols()) {
        // System.out.println("str=" + str);
        // }
    }

    private static String toLowerCaseAndFormat(String hiveSql) {
        if (null == hiveSql || 0 == hiveSql.length()) {
            return null;
        }

        hiveSql = hiveSql.trim();
        hiveSql = hiveSql.replaceAll("; *[sS][eE][tT]", "; set");
        hiveSql = hiveSql.replaceAll("; *[sS][eE][lL][eE][cC][tT]", "; select");
        hiveSql = hiveSql.replaceAll("; *[iI][nN][sS][eE][rR][tT]", "; insert");
        hiveSql = hiveSql.replaceAll("; *[sS][hH][oO][wW]", "; show");

        hiveSql = hiveSql.replaceAll("; *[dD][eE][sS][cC][rR][iI][bB][eE]", "; describe");
        hiveSql = hiveSql.replaceAll("; *[eE][xX][pP][lL][aA][iI][nN]", "; explain");

        hiveSql = hiveSql.replaceAll("; *[cC][rR][eE][aA][tT][eE]", "; create");
        hiveSql = hiveSql.replaceAll("; *[dD][rR][oO][pP]", "; drop");
        hiveSql = hiveSql.replaceAll("; *[aA][lL][eE][rR][tT]", "; alert");
        hiveSql = hiveSql.replaceAll("; *[tT][rR][uU][nN][cC][aA][tT][eE]", "; truncate");

        hiveSql = hiveSql.replaceAll("; *[lL][oO][aA][dD]", "; load");
        hiveSql = hiveSql.replaceAll("; *[uU][pP][dD][aA][tT][eE]", "; update");
        hiveSql = hiveSql.replaceAll("; *[dD][eE][lL][eE][tT][eE]", "; delete");
        hiveSql = hiveSql.replaceAll("; *[iI][mM][pP][oO][rR][tT]", "; import");
        hiveSql = hiveSql.replaceAll("; *[eE][xX][pP][oO][rR][tT]", "; export");

        return hiveSql;
    }

    private static boolean isMultiSql(String hiveScript) {
        return opers.stream().filter(oper -> hiveScript.toLowerCase().contains(oper)).count() > 0;
    }

    private static String sql$ReplaceTo0(String sql) {
        String rel = "(\\$)(\\{).*?(\\})";
        Pattern p = Pattern.compile(rel, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher m = p.matcher(sql);
        while (m.find()) {
            String str = m.group();
            sql = sql.replace(str, "0");
        }
        return sql;
    }

    private static void parse(String excute_script) throws ParseException {
        ParseDriver pd = new ParseDriver();
        ASTNode ast = pd.parse(excute_script);
        System.out.println(ast.toStringTree());
        Set<String> set = parseIteral(ast);
        for (String str : set) {
            System.out.println("--" + str);
        }
    }

    private static Set<String> parseIteral(ASTNode ast) {
        Set<String> set = new HashSet<String>();// 当前查询所对应到的表集合
        prepareToParseCurrentNodeAndChilds(ast);
        set.addAll(parseChildNodes(ast));
        set.addAll(parseCurrentNode(ast, set));
        endParseCurrentNode(ast);
        return set;
    }

    private static void prepareToParseCurrentNodeAndChilds(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {// join 从句开始
            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
                joinClause = true;
                break;
            case HiveParser.TOK_QUERY:
                tableNameStack.push(nowQueryTable);
                getOperStack().push(oper);
                nowQueryTable = "";
                oper = OPERATION_TOKON.SELECT;
                break;
            case HiveParser.TOK_INSERT:
                tableNameStack.push(nowQueryTable);
                getOperStack().push(oper);
                oper = OPERATION_TOKON.INSERT;
                break;
            /*
             * case HiveParser.TOK_DELETE_FROM: tableNameStack.push(nowQueryTable);
             * operStack.push(oper); oper = OPERATION_TOKON.DELETE; nowQueryTable =
             * BaseSemanticAnalyzer .getUnescapedName((ASTNode) ast.getChild(0));//sql22
             * break;
             */
            /*
             * case HiveParser.TOK_UPDATE_TABLE: tableNameStack.push(nowQueryTable);
             * operStack.push(oper); oper = OPERATION_TOKON.UPDATE; nowQueryTable =
             * BaseSemanticAnalyzer .getUnescapedName((ASTNode) ast.getChild(0));//sql22
             * break;
             */
            case HiveParser.TOK_SELECT:
                tableNameStack.push(nowQueryTable);
                getOperStack().push(oper);
                oper = OPERATION_TOKON.SELECT;
                break;
            case HiveParser.TOK_DROPTABLE:
                operMetaInfo = true;
                oper = OPERATION_TOKON.DROP;
                break;
            case HiveParser.TOK_TRUNCATETABLE:
                oper = OPERATION_TOKON.TRUNCATE;
                break;
            case HiveParser.TOK_LOAD:
                oper = OPERATION_TOKON.LOAD;
                break;
            case HiveParser.TOK_CREATETABLE:
                operMetaInfo = true;
                oper = OPERATION_TOKON.CREATETABLE;
                break;
            }
            if (ast.getToken().getType() >= HiveParser.TOK_ALTERDATABASE_PROPERTIES
                    && ast.getToken().getType() <= HiveParser.TOK_ALTERVIEW_RENAME) {
                operMetaInfo = true;
                oper = OPERATION_TOKON.ALTER;
            }
            if (ast.getToken().getType() >= HiveParser.TOK_DESCDATABASE
                    && ast.getToken().getType() <= HiveParser.TOK_DESCTABLE) {
                operMetaInfo = true;
                oper = OPERATION_TOKON.DESC;
            }
            if (ast.getToken().getType() >= HiveParser.TOK_SHOWCOLUMNS
                    && ast.getToken().getType() <= HiveParser.TOK_SHOW_TBLPROPERTIES) {
                oper = OPERATION_TOKON.SHOW;
            }
        }
    }

    private static Set<String> parseChildNodes(ASTNode ast) {
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

    private static Set<String> parseCurrentNode(ASTNode ast, Set<String> set) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
            case HiveParser.TOK_TABLE_PARTITION:
                if (ast.getChildCount() != 2) {
                    String table = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                    if (oper == OPERATION_TOKON.SELECT) {
                        nowQueryTable = table;
                    }
                    getTables().add(table + ":" + oper);
                }
                break;
            /*
             * case HiveParser.TOK_DELETE_FROM:// outputTable String deletetabl =
             * BaseSemanticAnalyzer .getUnescapedName((ASTNode) ast.getChild(0)); if (oper
             * == OPERATION_TOKON.DELETE) { nowQueryTable = deletetabl; }
             * tables.add(deletetabl + "\t" + oper); break;
             */
            /*
             * case HiveParser.TOK_UPDATE_TABLE:// outputTable String updatetab =
             * BaseSemanticAnalyzer .getUnescapedName((ASTNode) ast.getChild(0)); if (oper
             * == OPERATION_TOKON.UPDATE) { nowQueryTable = updatetab; }
             * tables.add(updatetab + "\t" + oper); break;
             */
            case HiveParser.TOK_TAB:// outputTable
                String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                if (oper == OPERATION_TOKON.SELECT) {
                    nowQueryTable = tableTab;
                }
                getTables().add(tableTab + ":" + oper);
                break;
            case HiveParser.TOK_TABREF:// inputTable
                ASTNode tabTree = (ASTNode) ast.getChild(0);
                String tableName = (tabTree.getChildCount() == 1)
                        ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                        : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "."
                                + tabTree.getChild(1);
                if (oper == OPERATION_TOKON.SELECT) {
                    if (joinClause && !"".equals(nowQueryTable)) {
                        nowQueryTable += "&" + tableName;//
                    } else {
                        nowQueryTable = tableName;
                    }
                    set.add(tableName);
                }
                getTables().add(tableName + ":" + oper);
                if (ast.getChild(1) != null) {
                    String alia = ast.getChild(1).getText().toLowerCase();
                    getTableAlias().put(alia, tableName);// sql6
                                                         // p别名在tabref只对应为一个表的别名。
                }
                break;
            case HiveParser.TOK_TABLE_OR_COL:
                if (ast.getParent().getType() != HiveParser.DOT) {
                    String col = ast.getChild(0).getText().toLowerCase();
                    if (getTableAlias().get(col) == null && getColAlais().get(nowQueryTable + "." + col) == null) {
                        if (nowQueryTable.indexOf("&") > 0) {// sql23
                            getCols().add(UNKNOWN + "." + col);
                        } else {
                            getCols().add(nowQueryTable + "." + col);
                        }
                    }
                }
                break;
            case HiveParser.TOK_ALLCOLREF:
                if (ast.getChildCount() > 0) {
                    ASTNode tree = (ASTNode) ast.getChild(0);
                    String selectTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tree);
                    if (tableAlias.get(selectTable) != null) {
                        selectTable = tableAlias.get(selectTable);
                    }
                    getCols().add(selectTable + ".*");
                } else {
                    getCols().add(nowQueryTable + ".*");
                }
                break;
            case HiveParser.TOK_FUNCTIONSTAR:
                getCols().add(nowQueryTable + ".*");
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
                    getTableAlias().put(tableAlias, aliaReal);
                }
                break;

            case HiveParser.TOK_SELEXPR:
                if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
                    String column = ast.getChild(0).getChild(0).getText().toLowerCase();
                    if (nowQueryTable.indexOf("&") > 0) {
                        getCols().add(UNKNOWN + "." + column);
                    } else if (getColAlais().get(nowQueryTable + "." + column) == null) {
                        getCols().add(nowQueryTable + "." + column);
                    }
                } else if (ast.getChild(1) != null) {
                    String columnAlia = ast.getChild(1).getText().toLowerCase();
                    getColAlais().put(nowQueryTable + "." + columnAlia, "");
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
                            if (!getTables().contains(alia + ":" + oper) && getTableAlias().get(alia) == null) {// [b
                                getTableAlias().put(alia, nowQueryTable);
                            }
                            if (getTables().contains(alia + ":" + oper)) {
                                realTable = alia;
                            } else if (getTableAlias().get(alia) != null) {
                                realTable = getTableAlias().get(alia);
                            }
                            if (realTable == null || realTable.length() == 0 || realTable.indexOf("&") > 0) {
                                realTable = UNKNOWN;
                            }
                            getCols().add(realTable + "." + column);

                        }
                    }
                }
                break;

            case HiveParser.TOK_ALTERTABLE_ADDPARTS:
            case HiveParser.TOK_ALTERTABLE_RENAME:
            case HiveParser.TOK_ALTERTABLE_ADDCOLS:
            case HiveParser.TOK_ALTERTABLE:
                ASTNode alterNode = (ASTNode) ast.getChild(0);
                if (alterNode.getToken().getType() == HiveParser.TOK_TABNAME) {
                    String alterTableName = BaseSemanticAnalyzer.getUnescapedName(alterNode);
                    tables.add(alterTableName + ":" + oper);
                }
                break;
            case HiveParser.TOK_CREATETABLE:
                ASTNode createNode = (ASTNode) ast.getChild(0);
                String createTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) createNode);
                tables.add(createTableName + ":" + "CREATETABLE");
                break;
            case HiveParser.TOK_DROPTABLE:
                ASTNode dropNode = (ASTNode) ast.getChild(0);
                String dropTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) dropNode);
                tables.add(dropTableName + ":" + "DROP");
                break;
            }
            if (ast.getToken().getType() >= HiveParser.TOK_SHOWCOLUMNS
                    && ast.getToken().getType() <= HiveParser.TOK_SHOW_TBLPROPERTIES) {
                ASTNode showNode = (ASTNode) ast.getChild(0);
                String showTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) showNode);
                tables.add(showTableName + ":" + oper);
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
                tables.add(descTableName + ":" + oper);
            }
        }
        return set;
    }

    private static void endParseCurrentNode(ASTNode ast) {
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
                oper = getOperStack().pop();
                break;
            }
        }
    }

    private static String unescapeIdentifier(String val) {
        if (val == null) {
            return null;
        }
        if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }

    public static Set<String> getOpers() {
        return opers;
    }

    public static void setOpers(Set<String> opers) {
        HiveParser1.opers = opers;
    }

    public static String getExcute_script() {
        return excute_script;
    }

    public static void setExcute_script(String excute_script) {
        HiveParser1.excute_script = excute_script;
    }

    public static Set<String> getTables() {
        return tables;
    }

    public static void setTables(Set<String> tables) {
        HiveParser1.tables = tables;
    }

    public static Map<String, String> getTableAlias() {
        return tableAlias;
    }

    public static void setTableAlias(Map<String, String> tableAlias) {
        HiveParser1.tableAlias = tableAlias;
    }

    public static Set<String> getCols() {
        return cols;
    }

    public static void setCols(Set<String> cols) {
        HiveParser1.cols = cols;
    }

    public static Map<String, String> getColAlais() {
        return colAlais;
    }

    public static void setColAlais(Map<String, String> colAlais) {
        HiveParser1.colAlais = colAlais;
    }

    public static Stack<OPERATION_TOKON> getOperStack() {
        return operStack;
    }

    public static void setOperStack(Stack<OPERATION_TOKON> operStack) {
        HiveParser1.operStack = operStack;
    }

    public static OPERATION_TOKON getOper() {
        return oper;
    }

    public static void setOper(OPERATION_TOKON oper) {
        HiveParser1.oper = oper;
    }

    public static boolean isJoinClause() {
        return joinClause;
    }

    public static void setJoinClause(boolean joinClause) {
        HiveParser1.joinClause = joinClause;
    }

    public static Stack<String> getTableNameStack() {
        return tableNameStack;
    }

    public static void setTableNameStack(Stack<String> tableNameStack) {
        HiveParser1.tableNameStack = tableNameStack;
    }

    public static String getNowQueryTable() {
        return nowQueryTable;
    }

    public static void setNowQueryTable(String nowQueryTable) {
        HiveParser1.nowQueryTable = nowQueryTable;
    }
}
