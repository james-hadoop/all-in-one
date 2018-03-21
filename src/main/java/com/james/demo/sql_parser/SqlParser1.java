package com.james.demo.sql_parser;

import java.util.List;

import com.james.common.util.JamesUtil;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.AddAliasesVisitor;
import net.sf.jsqlparser.util.SelectUtils;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SqlParser1 {

    public static void main(String[] args) throws JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse("SELECT * FROM tab1");
        System.out.println(stmt.toString());
        JamesUtil.printDivider();

        Statements stmts = CCJSqlParserUtil.parseStatements("SELECT * FROM tab1; SELECT * FROM tab2");
        System.out.println(stmts.toString());
        JamesUtil.printDivider();

        Expression expr = CCJSqlParserUtil.parseExpression("a*(5+mycolumn)");
        System.out.println(expr.toString());
        JamesUtil.printDivider();

        Statement statement = CCJSqlParserUtil.parse("SELECT * FROM MY_TABLE1");
        Select selectStatement = (Select) statement;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        JamesUtil.printList(tableList);
        JamesUtil.printDivider();

        Select select = (Select) CCJSqlParserUtil.parse("select a,b,c from test");
        final AddAliasesVisitor instance = new AddAliasesVisitor();
        select.getSelectBody().accept(instance);
        System.out.println(select.getSelectBody().toString());
        List<WithItem> withItemList = select.getWithItemsList();
        if (null != withItemList) {
            for (WithItem item : withItemList) {
                System.out.println(item);
            }
        }
        System.out.println();
        
        select = (Select) CCJSqlParserUtil.parse("select a from mytable");
        SelectUtils.addExpression(select, new Column("b"));
        System.out.println(select.getSelectBody().toString());
        withItemList = select.getWithItemsList();
        if (null != withItemList) {
            for (WithItem item : withItemList) {
                System.out.println(item);
            }
        }
        JamesUtil.printDivider();
    }

}
