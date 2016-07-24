package com.james.dataStructure.tree;

import java.util.List;

import com.james.common.util.JamesUtil;
import com.james.dataStructure.tree.BinTree.TreeNode;

public class BinTreeTest {

    public static void main(String[] args) {
        BinTree<String> tree = new BinTree<String>("root");
        TreeNode n1 = tree.addNode(tree.root(), "n1", true);
        TreeNode n2 = tree.addNode(tree.root(), "n2", false);
        TreeNode n3 = tree.addNode(n1, "n3", true);
        TreeNode n4 = tree.addNode(n1, "n4", false);
        TreeNode n5 = tree.addNode(n2, "n5", true);
        TreeNode n6 = tree.addNode(n2, "n6", false);

        System.out.println("deep: " + tree.deep());
        JamesUtil.printDivider();

        List<TreeNode> listPre = tree.preView();
        JamesUtil.printTreeList(listPre, "Pre");

        List<TreeNode> listIn = tree.inView();
        JamesUtil.printTreeList(listIn, "In");

        List<TreeNode> listPost = tree.postView();
        JamesUtil.printTreeList(listPost, "Post");
        JamesUtil.printDivider();

        List<TreeNode> listBreadthFirst = tree.breadthFirst();
        JamesUtil.printTreeList(listBreadthFirst, "Breadth First");
        JamesUtil.printDivider();
    }
}
