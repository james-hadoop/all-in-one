package com.james.mail.amazon.parser.conf;

public class MailConf {
    /*
     * receive mails
     */
    public static final String PROTOCOL = "pop3";
    public static final boolean IS_SSL = true;
    public static final String HOST = "pop.126.com";
    public static final int PORT = 995;
    public static final String USERNAME = "akajames@126.com";
    public static final String PASSWORD = "";

    /*
     * the code position of the website
     */
    public static final String CODE_UPSIDE = "Amazon.it,amazon.co.jp,amazon.de,amazon.fr";
    public static final String CODE_DOWNSIDE = "Amazon.ca,Amazon.es,Amazon.co.uk,Amazon.com";
}
