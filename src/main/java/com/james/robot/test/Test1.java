package com.james.robot.test;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import java.awt.AWTException;
import java.awt.BorderLayout;
import java.awt.Robot;
import javax.swing.JTextField;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

public class Test1 extends JFrame {

	public static void main(String[] args) {
		Test1 frame = new Test1();
		frame.setVisible(true);
	}

	private static final long serialVersionUID = 260649076714941022L;
	private JLabel label;
	private JTextField velocity;// �ٶ������
	private JButton button;
	private JLabel lbls;
	private Roll roll = null;// �Զ������߳�

	public Test1() {
		setAlwaysOnTop(true);// ʼ����ʾ����Ļ����
		setTitle("�Զ������Ķ�");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 284, 94);

		this.label = new JLabel("�����ٶ�");
		getContentPane().add(this.label, BorderLayout.WEST);

		this.velocity = new JTextField();
		this.velocity.setText("2");
		getContentPane().add(this.velocity, BorderLayout.CENTER);
		this.velocity.setColumns(10);

		this.button = new JButton("��ʼ/ֹͣ");
		this.button.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (roll == null) {
					try {
						roll = new Roll(Integer.parseInt(velocity.getText()));
					} catch (AWTException exception) {
						exception.printStackTrace();
						JOptionPane.showMessageDialog(Test1.this,
								"�����Զ�����ʧ��!");
						return;
					} catch (NumberFormatException exception) {
						exception.printStackTrace();
						JOptionPane.showMessageDialog(Test1.this, "��������!");
						return;
					}
					roll.startRoll();
				} else {
					roll.stopRoll();
					roll = null;
				}
			}
		});
		getContentPane().add(this.button, BorderLayout.EAST);

		this.lbls = new JLabel("�����ʼ��ť��,������Թ�������,2s��ʼ����");
		getContentPane().add(this.lbls, BorderLayout.SOUTH);
	}

}

/**
 * �Զ������߳�
 */
class Roll extends Thread {

	private int v;// �ٶ�
	private Robot robot;
	private boolean run;// �Ƿ���Ҫ��������,false��ʾֹͣ����

	public Roll(int v) throws AWTException {
		this.v = v;
		robot = new Robot();
	}

	@Override
	public void run() {
		super.run();
		do {
			robot.delay(2000);
			robot.mouseWheel(v);
		} while (this.run);
	}

	public void startRoll() {
		this.run = true;
		this.start();
	}

	public void stopRoll() {
		this.run = false;
	}

}
