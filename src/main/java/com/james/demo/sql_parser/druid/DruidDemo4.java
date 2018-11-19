package com.james.demo.sql_parser.druid;

import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcConstants;
import com.james.common.util.JamesUtil;

public class DruidDemo4 {

    public static void main(String[] args) {
        StringBuilder from =new StringBuilder();
        
        String dbType = JdbcConstants.MYSQL;

        String sql = "SELECT tt.a_a AS f_a_a, tt.b_b AS f_b_b, tt.b_c AS f_b_c, at_b.same\n" + "FROM (\n"
                + "    SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b\n"
                + "        , MAX(a_c) AS a_c, MAX(same) AS same\n" + "    FROM t_a\n" + "    WHERE a_c = 3\n"
                + "    GROUP BY a_key\n" + "    ORDER BY a_a\n" + ") at_a\n" + "    LEFT JOIN (\n"
                + "        SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b\n"
                + "            , MAX(b_c) AS b_c, MAX(same) AS b_same\n" + "        FROM t_b\n"
                + "        GROUP BY b_key\n" + "        ORDER BY b_b\n" + "    ) at_b\n"
                + "    ON at_a.a_key = at_b.b_key AS tt";

        sql=sql.toLowerCase();
        System.out.println("sql: \n\t" + sql);
        
        System.out.println("--> parseStatements(sql, dbType)");
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        MySqlSchemaStatVisitor visitor2 = new MySqlSchemaStatVisitor();
        MySqlASTVisitorAdapter astVisitor = new MySqlASTVisitorAdapter();
        SQLASTOutputVisitor astOutputVisitor = SQLUtils.createFormatOutputVisitor(from, statementList, dbType);

        SQLSelectStatement statement = (SQLSelectStatement) statementList.get(0);

        statement.accept(visitor);
        // visitor.getColumns():
        System.out.println("\n--> visitor.getColumns():");
        System.out.println(visitor.getColumns());

        // SQLSelect
        SQLSelect sqlSelect = statement.getSelect();

        // SQLSelectQuery
        SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
        sqlSelectQuery.accept(visitor);
        // visitor.getColumns():
        System.out.println("\n--> visitor.getColumns():");
        System.out.println(visitor.getColumns());

        // SQLSelectQueryBlock
        SQLSelectQueryBlock queryBlock = sqlSelect.getQueryBlock();
        
//        System.out.println("\n--> from:");
//        queryBlock.getFrom().accept(astOutputVisitor);
//        System.out.println(from.toString());
        
        List<SQLSelectItem> selectItemList=queryBlock.getSelectList();
        for(SQLSelectItem item:selectItemList) {
            if(from.length()>1){  
                from.append(",") ;  
            }  
            item.accept(astOutputVisitor);
        }
        System.out.println("\n-->SELECT:\n "+from) ;  

        // SQLTableSource
        System.out.println("\n--> queryBlock.findTableSourceWithColumn(\"b_same\"):");
        SQLTableSource tableSource = queryBlock.findTableSourceWithColumn("b_same");
        System.out.println("tableSource: " + tableSource.getAlias());

        // SQLSelectItem
        SQLSelectItem selectItem = queryBlock.findAllColumnSelectItem();
    }
}
