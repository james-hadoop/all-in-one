package com.james.dataStructure.tree;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class BinTree<E> {
    public static class TreeNode {
        public Object data;
        public TreeNode left;
        public TreeNode right;
        public TreeNode parent;

        public TreeNode() {

        }

        public TreeNode(Object data) {
            this.data = data;
        }
    }

    private TreeNode root;

    public BinTree() {

    }

    public BinTree(E data) {
        this.root = new TreeNode(data);
    }

    public boolean isEmpty() {
        return null == root;
    }

    public TreeNode root() {
        return root;
    }

    public TreeNode addNode(TreeNode parent, E data, boolean isLeft) {
        if (parent == null) {
            throw new RuntimeException(parent + "node is null");
        }
        if (true == isLeft && parent.left != null) {
            throw new RuntimeException(parent + "left is not null");
        }
        if (false == isLeft && parent.right != null) {
            throw new RuntimeException(parent + "right is not null");
        }

        TreeNode newNode = new TreeNode(data);
        newNode.parent = parent;

        if (true == isLeft) {
            parent.left = newNode;
        } else {
            parent.right = newNode;
        }

        return newNode;
    }

    public int deep() {
        return deep(root);
    }

    private int deep(TreeNode node) {
        if (root == null) {
            return 0;
        }
        if (node.left == null & node.right == null) {
            return 1;
        } else {
            int deepLeft = deep(node.left);
            int deepRight = deep(node.right);
            int deep = deepLeft > deepRight ? deepLeft : deepRight;
            return deep + 1;
        }
    }

    public List<TreeNode> preView() {
        return preView(root);
    }

    private List<TreeNode> preView(TreeNode node) {
        List<TreeNode> listTreeNode = new ArrayList<TreeNode>();

        listTreeNode.add(node);
        if (node.left != null) {
            listTreeNode.addAll(preView(node.left));
        }
        if (node.right != null) {
            listTreeNode.addAll(preView(node.right));
        }

        return listTreeNode;
    }

    public List<TreeNode> inView() {
        return inView(root);
    }

    private List<TreeNode> inView(TreeNode node) {
        List<TreeNode> listTreeNode = new ArrayList<TreeNode>();

        if (node.left != null) {
            listTreeNode.addAll(inView(node.left));
        }

        listTreeNode.add(node);

        if (node.right != null) {
            listTreeNode.addAll(inView(node.right));
        }

        return listTreeNode;
    }

    public List<TreeNode> postView() {
        return postView(root);
    }

    private List<TreeNode> postView(TreeNode node) {
        List<TreeNode> listTreeNode = new ArrayList<TreeNode>();

        if (node.left != null) {
            listTreeNode.addAll(inView(node.left));
        }

        if (node.right != null) {
            listTreeNode.addAll(inView(node.right));
        }

        listTreeNode.add(node);

        return listTreeNode;
    }

    public List<TreeNode> breadthFirst() {
        List<TreeNode> listTreeNode = new ArrayList<TreeNode>();
        Queue<TreeNode> queue = new ArrayDeque<TreeNode>();

        queue.offer(root);

        while (queue.isEmpty() == false) {
            listTreeNode.add(queue.peek());
            TreeNode node = queue.poll();
            if (node.left != null) {
                queue.offer(node.left);
            }
            if (node.right != null) {
                queue.offer(node.right);
            }
        }

        return listTreeNode;
    }
}
