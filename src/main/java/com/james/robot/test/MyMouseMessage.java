package com.james.robot.test;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

public class MyMouseMessage extends JFrame {
	public MyMouseMessage() {
		MessagePanel p = new MessagePanel("MyMouseMessage");
		setLayout(new BorderLayout());
		add(p);

	}

	public static void main(String[] args) {
		MyMouseMessage frame = new MyMouseMessage();
		frame.setTitle("MyMouseMessage");
		frame.setSize(600, 400);
		frame.setLocationRelativeTo(null);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setVisible(true);
	}

	static class MessagePanel extends JPanel {
		private String message = "MyMouseMessage";
		private int x = 1600;
		private int y = 1400;

		public MessagePanel(String s) {
			message = s;
			addMouseListener(new MouseAdapter() {
				public void mouseClicked(MouseEvent e) {
					System.out.println("mouseClicked()");

					x = e.getX();
					y = e.getY();

					System.out.println("x=" + x + "\ty=" + y);
					repaint();
				}

				// public void mousePressed(MouseEvent e) {
				// System.out.println("mousePressed()");
				//
				// x = e.getX();
				// y = e.getY();
				//
				// System.out.println("x=" + x + "\ty=" + y);
				// // repaint();
				// }
				//
				// public void mouseReleased(MouseEvent e) {
				// System.out.println("mouseReleased()");
				//
				// x = e.getX();
				// y = e.getY();
				//
				// System.out.println("x=" + x + "\ty=" + y);
				// // repaint();
				// }
			});

			addMouseMotionListener(new MouseAdapter() {
				public void mouseMoved(MouseEvent e) {
					System.out.println("mouseMoved()");

					x = e.getX();
					y = e.getY();

					System.out.println("x=" + x + "\ty=" + y);
					// repaint();
				}

				public void mouseDragged(MouseEvent e) {
					System.out.println("mouseDragged()");

					x = e.getX();
					y = e.getY();

					System.out.println("x=" + x + "\ty=" + y);
					// repaint();
				}
			});
		}

		protected void paintComponent(Graphics g) {
			super.paintComponent(g);
			g.drawString(message, x, y);
		}

	}
}
