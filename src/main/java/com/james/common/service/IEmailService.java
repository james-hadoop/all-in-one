package com.james.common.service;

import org.apache.commons.mail.EmailException;

public interface IEmailService {
    void sendEmail(String mailAddress, String title, String content) throws EmailException;
}
