package com.james.demo.ssh;

import java.io.InputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class SshAuthInfo {

    private String host;
    private int port;
    private String username;
    private String password;

    public SshAuthInfo() {

    }

    public SshAuthInfo(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public static void main(String[] arg) throws Exception {
        SshAuthInfo info = new SshAuthInfo("JamesUbuntu", 22, "james", "james20061010");

        JSch jsch = new JSch();

        Session session = jsch.getSession(info.getUsername(), info.getHost(), info.getPort());
        session.setPassword(info.getPassword());

        session.setConfig("StrictHostKeyChecking", "no");

        session.connect();

        String command = "cp /home/james/logs/Template.log /home/james/logs/Template-dup.log";
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);
        channel.setInputStream(null);

        InputStream in = channel.getInputStream();

        channel.connect();

        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0)
                    break;
                System.out.print(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                if (in.available() > 0)
                    continue;
                System.out.println("exit-status: " + channel.getExitStatus());
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ee) {
            }
        }
        channel.disconnect();
        session.disconnect();
    }
}