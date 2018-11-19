package com.james.demo.sql_parser.druid;

import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcConstants;

public class DruidDemo5 {

    public static void main(String[] args) {
        String dbType = JdbcConstants.MYSQL;

//        String sql = "SELECT tt.a_a AS f_a_a, tt.b_b AS f_b_b, tt.b_c AS f_b_c, at_b.same\n" + "FROM (\n"
//                + "    SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b\n"
//                + "        , MAX(a_c) AS a_c, MAX(same) AS same\n" + "    FROM t_a\n" + "    WHERE a_c = 3\n"
//                + "    GROUP BY a_key\n" + "    ORDER BY a_a\n" + ") at_a\n" + "    LEFT JOIN (\n"
//                + "        SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b\n"
//                + "            , MAX(b_c) AS b_c, MAX(same) AS b_same\n" + "        FROM t_b\n"
//                + "        GROUP BY b_key\n" + "        ORDER BY b_b\n" + "    ) at_b\n"
//                + "    ON at_a.a_key = at_b.b_key AS tt";

        String sql = "SELECT p, s.count AS views\n" + 
                "    , (\n" + 
                "        SELECT COUNT(*)\n" + 
                "        FROM Comments rc\n" + 
                "        WHERE rc.linkedId = p.id\n" + 
                "            AND rc.classcode = 'InfoPublishs'\n" + 
                "    ) AS commentNumber\n" + 
                "    , (\n" + 
                "        SELECT COUNT(*)\n" + 
                "        FROM CollectIndexs rci\n" + 
                "        WHERE (rci.toId = p.id\n" + 
                "            AND rci.classcode = 'InfoPublishs'\n" + 
                "            AND rci.type = 'favorite')\n" + 
                "    ) AS favorite\n" + 
                "FROM InfoPublishs p, UserScores s\n" + 
                "WHERE (p.id = s.linkedId\n" + 
                "    AND p.userInfo.id = s.userInfo.id\n" + 
                "    AND s.classCode = 'InfoPublishs'\n" + 
                "    AND p.status = 1)\n" + 
                "ORDER BY p.createtime DESC";

        System.out.println("sql: \n\t" + sql);

        StringBuffer select = new StringBuffer();
        StringBuffer from = new StringBuffer();
        StringBuffer where = new StringBuffer();

        // parser得到AST
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList(); //

        // 将AST通过visitor输出
        SQLASTOutputVisitor visitor = SQLUtils.createFormatOutputVisitor(from, stmtList, dbType);
        SQLASTOutputVisitor whereVisitor = SQLUtils.createFormatOutputVisitor(where, stmtList, dbType);

        for (SQLStatement stmt : stmtList) {
//             stmt.accept(visitor);   
            if (stmt instanceof SQLSelectStatement) {
                SQLSelectStatement sstmt = (SQLSelectStatement) stmt;
                SQLSelect sqlselect = sstmt.getSelect();
                SQLSelectQueryBlock query = (SQLSelectQueryBlock) sqlselect.getQuery();

                query.getFrom().accept(visitor);
                query.getWhere().accept(whereVisitor);
            }
        }

        System.out.println(from.toString());
        System.out.println(select);
        System.out.println(where);
    }
}
