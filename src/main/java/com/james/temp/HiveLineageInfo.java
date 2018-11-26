package com.james.temp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HiveLineageInfo implements NodeProcessor {
    /**
     * Stores input tables in sql.
     */
    TreeSet inputTableList = new TreeSet();
    /**
     * Stores output tables in sql.
     */
    TreeSet OutputTableList = new TreeSet();

    /**
     *
     * @return java.util.TreeSet
     */
    public TreeSet getInputTableList() {
        return inputTableList;
    }

    /**
     * @return java.util.TreeSet
     */
    public TreeSet getOutputTableList() {
        return OutputTableList;
    }

    /**
     * Implements the process method for the NodeProcessor interface.
     */
    public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
            throws SemanticException {
        ASTNode pt = (ASTNode) nd;

        switch (pt.getToken().getType()) {

        case HiveParser.TOK_CREATETABLE:
            OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
            break;
        case HiveParser.TOK_TAB:
            OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
            break;

        case HiveParser.TOK_TABREF:
            ASTNode tabTree = (ASTNode) pt.getChild(0);
            String table_name = (tabTree.getChildCount() == 1)
                    ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                    : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
            inputTableList.add(table_name);
            break;
        }
        return null;
    }

    /**
     * parses given query and gets the lineage info.
     *
     * @param query
     * @throws ParseException
     */
    public void getLineageInfo(String query) throws ParseException, SemanticException {

        /*
         * Get the AST tree
         */
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }

        /*
         * initialize Event Processor and dispatcher.
         */
        inputTableList.clear();
        OutputTableList.clear();

        // create a walker which walks the tree in a DFS manner while maintaining
        // the operator stack. The dispatcher
        // generates the plan from the operator tree
        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        // The dispatcher fires the processor corresponding to the closest matching
        // rule and passes the context along
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);

        // Create a list of topop nodes
        ArrayList topNodes = new ArrayList();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }

    public static void main(String[] args) throws IOException, ParseException, SemanticException {
        //String query = "INSERT OVERWRITE TABLE liuxiaowen.lxw3 SELECT a.url FROM liuxiaowen.lxw1 a join liuxiaowen.lxw2 b ON (a.url = b.domain)";
        String query="SELECT tt.a_a AS f_a_a, tt.b_b AS f_b_b, tt.b_c AS f_b_c, at_b.b_same\n" + "FROM (\n"
                + "    SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b\n"
                + "        , MAX(a_c) AS a_c, MAX(same) AS same\n" + "    FROM t_a\n" + "    WHERE a_c = 3\n"
                + "    GROUP BY a_key\n" + "    ORDER BY a_a\n" + ") at_a\n" + "    LEFT JOIN (\n"
                + "        SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b\n"
                + "            , MAX(b_c) AS b_c, MAX(same) AS b_same\n" + "        FROM t_b\n"
                + "        GROUP BY b_key\n" + "        ORDER BY b_b\n" + "    ) at_b\n"
                + "    ON at_a.a_key = at_b.b_key AS tt";
        
        HiveLineageInfo lep = new HiveLineageInfo();
        lep.getLineageInfo(query);
        System.out.println("Input tables = " + lep.getInputTableList());
        System.out.println("Output tables = " + lep.getOutputTableList());
        
        System.out.println(lep.toString());
        
    }
}
