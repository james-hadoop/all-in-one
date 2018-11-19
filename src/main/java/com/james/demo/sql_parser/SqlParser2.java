package com.james.demo.sql_parser;

import java.util.List;

import com.james.common.util.JamesUtil;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.AddAliasesVisitor;

public class SqlParser2 {

    public static void main(String[] args) throws JSQLParserException {
        String sql = new StringBuilder(
                "SELECT at_a.a_a, at_b.b_b\n" + "FROM (\n" + "    SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b\n"
                        + "        , MAX(a_c) AS a_c\n" + "    FROM t_a\n" + "    GROUP BY a_key\n" + "    ORDER BY a_c\n"
                        + ") at_a\n" + "    LEFT JOIN (\n" + "        SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b\n"
                        + "            , MAX(b_c) AS b_c\n" + "        FROM t_b\n" + "        GROUP BY b_key\n"
                        + "        ORDER BY b_c\n" + "    ) at_b\n" + "    ON at_a.a_key = at_b.b_key")
                        .toString();
        System.out.println("sql: \n\t" + sql);

        Select select = (Select) CCJSqlParserUtil.parse(sql);
        final AddAliasesVisitor instance = new AddAliasesVisitor();
        select.getSelectBody().accept(instance);
        System.out.println(select.getSelectBody().toString());
        System.out.println();
        List<WithItem> withItemList = select.getWithItemsList();
        if (null != withItemList) {
            for (WithItem item : withItemList) {
                System.out.println(item);
            }
        }
        System.out.println();
        JamesUtil.printDivider();
    }
}
