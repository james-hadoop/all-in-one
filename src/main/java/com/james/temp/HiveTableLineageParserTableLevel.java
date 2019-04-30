package com.james.temp;

import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import com.james.common.util.JamesUtil;


public class HiveTableLineageParserTableLevel {
	private final static String sqlDemo = "INSERT INTO TABLE f_tt SELECT at_a.a_a AS f_a_a, at_b.b_b AS f_b_b, at_b.b_c AS f_b_c FROM(SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b, MAX(a_c) AS a_c FROM t_a WHERE a_c = 3 GROUP BY a_key ORDER BY a_a) at_a LEFT JOIN (SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b, MAX(b_c) AS b_c FROM t_b GROUP BY b_key ORDER BY b_b) at_b ON at_a.a_key = at_b.b_key";
	private final static String sqlDemo_1 = "INSERT INTO TABLE f_tt SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b, MAX(a_c) AS a_c FROM t_a WHERE a_c = 3 GROUP BY a_key ORDER BY a_a";

	private final static String sql52 = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_a.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
	private final static String sql52_b = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_b.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";

	// ParseDriver
	private static ParseDriver pd = new ParseDriver();

	// TableLevelMap
	Map<String, ArrayList<Pair<String, String>>> tableLevelMap = new HashMap<String, ArrayList<Pair<String, String>>>();
	private final static String ROOT = "root";
	private static Map<String, TableLevelNode> tableLevelNodeMap = new ConcurrentHashMap<String, TableLevelNode>();
	private static Set<String> topLevelTableSet = new HashSet<String>();

	public static void main(String[] args) throws IOException {
		String parsesql = sqlDemo;
		System.out.println(parsesql);
		init();

		ASTNode ast = null;
		try {
			ast = pd.parse(parsesql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(ast.toStringTree());
		JamesUtil.printDivider();

		// parseIteral
		parseIteral(ast);

		destroy();
	}

	public static void parseIteral(ASTNode ast) {
		parseCurrentNode(ast);
		parseChildNodes(ast);
//		parseCurrentNode(ast);
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

	private static void parseCurrentNode(ASTNode ast) {
		if (ast.getToken() != null) {

			switch (ast.getToken().getType()) {

			case HiveParser.TOK_LEFTOUTERJOIN:
			case HiveParser.TOK_RIGHTOUTERJOIN:
			case HiveParser.TOK_JOIN:
//				 System.out.println("HiveParser.TOK_JOIN:");

				// parent table
				System.out.println(ast.getParent().getText() + " -- " + ast.getParent().getParent().getText() + " -- "
						+ ast.getParent().getParent().getParent().getText());

				int parentHiveParserType = ast.getParent().getType();
				String parentTableName = 2 > ast.getParent().getChildCount()
						? ast.getParent().getParent().getParent().getChild(1).getText()
						: ast.getParent().getChild(1).getChild(1).getText();
				System.out.println("parentTable=" + parentTableName);

				// child table
				for (int i = 0; i < ast.getChildCount(); i++) {
					String tableName = ast.getChild(i).getChild(1).getText();
					int tableType = ast.getChild(i).getChild(1).getType();

					if (tableType == HiveParser.Identifier) {
						addTableLevelNodeToMap(tableName, parentTableName, parentHiveParserType);
					}
				}

//				String childTable1 = ast.getChild(0).getChild(1).getText();
//				String childTable2 = ast.getChild(1).getChild(1).getText();
//				System.out.println("childTable:\n\t" + childTable1 + " -- " + childTable2);
//				System.out.println("childTableType:\n\t" + ast.getChild(0).getChild(1).getType() + " -- " + ast.getChild(1).getChild(1).getType());
//
//				System.out.println("ast.getChild(0).getText(): " + ast.getChild(0).getText());
//				System.out.println("\tast.getChild(0).getChild(0).getText(): " + ast.getChild(0).getChild(0).getText());
//				System.out.println("\tast.getChild(0).getChild(1).getText(): " + ast.getChild(0).getChild(1).getText());
//				System.out.println("ast.getChild(1).getText(): " + ast.getChild(1).getText());
//				System.out.println("\tast.getChild(1).getChild(0).getText(): " + ast.getChild(1).getChild(0).getText());
//				System.out.println("\tast.getChild(1).getChild(1).getText(): " + ast.getChild(1).getChild(1).getText());
				System.out.println("--------------------------");

				break;
//			case HiveParser.TOK_TABREF:
//				System.out.println("HiveParser.TOK_TABREF:");
//				break;
			default:
				break;
			}
		}
	}

	private static void init() {
		TableLevelNode node = new TableLevelNode("<EOF>", 0, null);

		tableLevelNodeMap.put("<EOF>", node);
	}

	public static void destroy() {
		JamesUtil.printDivider("destroy()");
		for (Entry<String, TableLevelNode> e : tableLevelNodeMap.entrySet()) {
			if (1 == e.getValue().getLevel()) {
				topLevelTableSet.add(e.getKey());
			}

			if (e.getKey().equals("<EOF>")) {
				continue;
			}

			System.out.println(
					e.getKey() + " -- " + e.getValue().getLevel() + " : " + e.getValue().getParent().getTableName());

			JamesUtil.printDivider("topLevelTableSet");
			JamesUtil.printSet(topLevelTableSet);
		}
	}

	private static void addTableLevelNodeToMap(String tableName, String parentTableName, int parentHiveParserType) {
		if (null == tableName || null == parentTableName) {
			return;
		}

		if (parentHiveParserType == HiveParser.TOK_FROM) {
			// next level table
			TableLevelNode node = new TableLevelNode(tableName, tableLevelNodeMap.get(parentTableName).getLevel() + 1,
					tableLevelNodeMap.get(parentTableName));
			tableLevelNodeMap.put(tableName, node);
		} else {
			// current level table
			String grandParentTableName=tableLevelNodeMap.get(parentTableName).getParent().getTableName();
			
			TableLevelNode node = new TableLevelNode(tableName, tableLevelNodeMap.get(parentTableName).getLevel(),
					tableLevelNodeMap.get(grandParentTableName));
			tableLevelNodeMap.put(tableName, node);
		}
	}

	public static ASTNode getFromTok(ASTNode ast) {
		if (null != ast.getChildren() || 0 != ast.getChild(0).getChildCount()) {
			return (ASTNode) ast.getChild(0).getChild(0);
		}

		return null;
	}
}