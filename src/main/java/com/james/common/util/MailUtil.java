package com.james.common.util;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

public class MailUtil {
    private String mailHost;
    private String mailFrom;
    private String mailUserName;
    private String mailPwd;
    private int port;

    public void sendMail(String mailList, String subject, String body) throws EmailException {
        HtmlEmail email = new HtmlEmail();
        email.setAuthentication(this.mailUserName, this.mailPwd);
        email.setHostName(this.mailHost);
        //email.setSmtpPort(this.port);
        for (String mail : mailList.split(",")) {
            email.addTo(mail);
        }
        email.setFrom(this.mailFrom);
        email.setSubject(subject);
        email.setCharset("GB2312");
        email.setHtmlMsg(body);
        email.send();
    }

    public String getMailHost() {
        return this.mailHost;
    }

    public void setMailHost(String mailHost) {
        this.mailHost = mailHost;
    }

    public String getMailFrom() {
        return this.mailFrom;
    }

    public void setMailFrom(String mailFrom) {
        this.mailFrom = mailFrom;
    }

    public String getMailUserName() {
        return this.mailUserName;
    }

    public void setMailUserName(String mailUserName) {
        this.mailUserName = mailUserName;
    }

    public String getMailPwd() {
        return this.mailPwd;
    }

    public void setMailPwd(String mailPwd) {
        this.mailPwd = mailPwd;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
