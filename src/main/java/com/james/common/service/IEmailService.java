package com.james.common.service;

import org.apache.commons.mail.EmailException;

public interface IEmailService {
    void sendEmail(String mailBox, String title, String content) throws EmailException;
}
