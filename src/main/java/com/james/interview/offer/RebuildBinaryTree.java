package com.james.interview.offer;

public class RebuildBinaryTree {
    public static void main(String[] args) {
        // preorder: {1,2,4,7,3,5,6,8}
        // inorder: {4,7,2,1,5,3,8,6}
        int[] preorder = { 1, 2, 4, 7, 3, 5, 6, 8 };
        int[] inorder = { 4, 7, 2, 1, 5, 3, 8, 6 };

        BinaryTreeNode tree = constuctBinaryTree(preorder, 0, preorder.length - 1, inorder, 0, inorder.length - 1);

        printBinaryTree(tree);
    }

    private static BinaryTreeNode constuctBinaryTree(int[] preorder, int preorderStart, int preorderEnd, int[] inorder,
            int inorderStart, int inorderEnd) {

        if (preorderStart > preorderEnd || inorderStart > inorderEnd) {
            return null;
        }

        if (preorderStart == preorderEnd) {
            return new BinaryTreeNode(preorder[preorderStart]);
        }

        if (inorderStart == inorderEnd) {
            return new BinaryTreeNode(preorder[inorderStart]);
        }

        int value = preorder[preorderStart];
        BinaryTreeNode root = new BinaryTreeNode(value);

        int inorderRootIndex = -1;
        for (int i = inorderStart; i < inorderEnd; i++) {
            if (inorder[i] == value) {
                inorderRootIndex = i;
                break;
            }
        }
        
        int leftTreelength=inorderRootIndex-inorderStart;
        int rightTreelength=inorderEnd-inorderRootIndex;

        root.left = constuctBinaryTree(preorder, preorderStart + 1, preorderStart + 1+leftTreelength, inorder, inorderStart,
                inorderRootIndex - 1);
        root.right = constuctBinaryTree(preorder, preorderEnd-rightTreelength, preorderEnd, inorder, inorderRootIndex + 1,
                inorderEnd);

        return root;
    }

    private static void printBinaryTree(BinaryTreeNode root) {
        if (null != root) {
            System.out.println(root.value);
        }

        if (null != root.left) {
            printBinaryTree(root.left);
        }

        if (null != root.right) {
            printBinaryTree(root.right);
        }
    }
}

class BinaryTreeNode {
    int value;
    BinaryTreeNode left;
    BinaryTreeNode right;

    public BinaryTreeNode(int value) {
        this.value = value;
    }
}
