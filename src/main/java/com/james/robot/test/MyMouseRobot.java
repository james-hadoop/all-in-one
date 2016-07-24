package com.james.robot.test;

import java.awt.AWTException;
import java.awt.Graphics;
import java.awt.Robot;
import java.awt.event.InputEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.JFrame;
import javax.swing.JPanel;

public class MyMouseRobot extends JFrame {
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

	public static void main(String[] args) {
		MyMouseMessage frame = new MyMouseMessage();
		frame.setTitle("MyMouseMessage");
		frame.setBounds(0, 0, 800, 400);
		// frame.setLocation(0, 0);
		// frame.setSize(800, 400);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setVisible(true);

		System.out.println("MouseMotion().start()");
		new MouseMotion().start();
	}
}

class MouseMotion extends Thread {
	private int x;
	private int y;
	private Robot robot;

	public MouseMotion() {
		try {
			robot = new Robot();
		} catch (AWTException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run() {
		for (int i = 100; i < 300; i = i + 50) {
			x = i * 2;
			y = i;

			System.out.println("robot.mouseMove(" + x + "," + y + ")");
			robot.mouseMove(x, y);
			robot.mousePress(InputEvent.BUTTON1_DOWN_MASK);
			robot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);
		}
	}
}
