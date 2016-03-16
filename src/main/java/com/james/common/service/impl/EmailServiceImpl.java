package com.james.common.service.impl;

import org.apache.commons.mail.EmailException;

import com.fangdd.esf.mail.MailHelper;
import com.james.common.conf.CommonConf;
import com.james.common.service.IEmailService;

public class EmailServiceImpl implements IEmailService {
    public void sendEmail(String mailBox, String title, String content) throws EmailException {
        if (null == mailBox || 0 == mailBox.length()) {
            return;
        }

        MailHelper mailHelper = new MailHelper();
        mailHelper.setMailFrom(CommonConf.MAIL_FROM);
        mailHelper.setMailHost(CommonConf.MAIL_HOST);
        mailHelper.setMailUserName(CommonConf.MAIL_USERNAME);
        mailHelper.setMailPwd(CommonConf.MAIL_PASSWORD);

        mailHelper.SendMail(mailBox, title, content);
    }
}
