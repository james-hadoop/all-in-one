package com.james.common.service.impl;

import java.util.Date;

import org.apache.commons.mail.EmailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.james.common.service.IEmailService;
import com.james.common.util.MailUtil;

public class EmailServiceImpl implements IEmailService {
    private static Logger logger = LoggerFactory.getLogger(EmailServiceImpl.class);
    private String mailFrom;
    private String mailHost;
    private String mailUserName;
    private String mailPassword;
    private int mailPort;
    
    public EmailServiceImpl(String mailFrom, String mailHost, String mailUserName, String mailPassword,String mailPort) {
        this.mailFrom = mailFrom;
        this.mailHost = mailHost;
        this.mailUserName = mailUserName;
        this.mailPassword = mailPassword;
        this.mailPort =  Integer.valueOf(mailPort);
    }

    @Override
    public void sendEmail(String mailAddress, String title, String content) throws EmailException {
        MailUtil MailUtil = new MailUtil();
        MailUtil.setMailFrom(mailFrom);
        MailUtil.setMailHost(mailHost);
        MailUtil.setMailUserName(mailUserName);
        MailUtil.setMailPwd(mailPassword);
        //MailUtil.setPort(mailPort);
        if (mailAddress.contains(",")) {
            String[] mailBox = mailAddress.split(",");
            if (null != mailBox && mailBox.length > 0) {
                for (int i = 0; i < mailBox.length; i++) {
                    try {
                        MailUtil.sendMail(mailBox[i], title, content);
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error("Message sending failed:" + e.toString());
                        throw e;
                    }
                }
            }
        } else {
            MailUtil.sendMail(mailAddress, title, content);
        }

        logger.info(new Date() + "Mail complete");
        MailUtil = null;
    }
}
