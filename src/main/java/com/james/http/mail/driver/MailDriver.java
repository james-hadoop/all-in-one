package com.james.http.mail.driver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.mail.BodyPart;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.NoSuchProviderException;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.Store;

import com.james.common.util.DateUtil;
import com.james.http.mail.conf.MailConf;

public class MailDriver {
    public static void main(String[] args) throws Exception {
        List<Map<String, String>> listMapCodeMoney = new ArrayList<Map<String, String>>();

        Properties props = new Properties();
        props.put("mail.pop3.ssl.enable", MailConf.IS_SSL);
        props.put("mail.pop3.host", MailConf.HOST);
        props.put("mail.pop3.port", MailConf.PORT);

        Session session = Session.getDefaultInstance(props);

        Store store = null;
        Folder folder = null;
        try {
            store = session.getStore(MailConf.PROTOCOL);
            store.connect(MailConf.USERNAME, MailConf.PASSWORD);

            folder = store.getFolder("INBOX");
            folder.open(Folder.READ_ONLY);

            Message[] arrMessage = folder.getMessages();
            List<Message> listMessage = Arrays.asList(arrMessage);
            List<Message> listLatest100Message = listMessage.subList(listMessage.size() - 200, listMessage.size());

            String strYesterday = DateUtil.getLastNDay(1);
            StringBuffer bodytext = new StringBuffer();
            for (Message message : listLatest100Message) {
                String strReceiveDate = DateUtil.DateToString(message.getSentDate());
                System.out.println(strYesterday + " Vs " + strReceiveDate);

                if (strYesterday.compareTo(strReceiveDate) > 0) {
                    continue;
                }

                if (message.getContent() instanceof Multipart) {
                    Multipart mp = (Multipart) message.getContent();
                    for (int t = 0; t < 1; t++) {
                        BodyPart part = mp.getBodyPart(t);
                        bodytext.append(getMailContent(part));
                    }
                } else if (message.isMimeType("text/html")) {
                    System.out.println("text/html");
                    continue;
                } else {
                    System.out.println("else");
                    continue;
                }

                String mailContent = bodytext.toString();
                // System.out.println("mailContext: " + mailContent);
                bodytext.delete(0, bodytext.length());

                Map<String, String> mapCodeMoney = extractCodeAndMoney(mailContent);
                if (null != mapCodeMoney && 0 < mapCodeMoney.size()) {
                    listMapCodeMoney.add(mapCodeMoney);
                }
            }

            if (null != listMapCodeMoney && 0 < listMapCodeMoney.size()) {
                write2File(listMapCodeMoney);
            }
        } catch (NoSuchProviderException e) {
            e.printStackTrace();
        } catch (MessagingException e) {
            e.printStackTrace();
        } finally {
            try {
                if (folder != null) {
                    folder.close(false);
                }
                if (store != null) {
                    store.close();
                }
            } catch (MessagingException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 解析邮件，把得到的邮件内容保存到一个StringBuffer对象中，解析邮件 主要是根据MimeType类型的不同执行不同的操作，一步一步的解析
     */
    private static String getMailContent(Part part) throws Exception {
        StringBuffer bodytext = new StringBuffer();

        String contenttype = part.getContentType();
        int nameindex = contenttype.indexOf("name");
        boolean conname = false;
        if (nameindex != -1)
            conname = true;
        System.out.println("CONTENTTYPE: " + contenttype);
        if (part.isMimeType("text/plain") && !conname) {
            bodytext.append((String) part.getContent());
        } else if (part.isMimeType("text/html") && !conname) {
            bodytext.append((String) part.getContent());
        } else if (part.isMimeType("multipart/*")) {
            Multipart multipart = (Multipart) part.getContent();
            int counts = multipart.getCount();
            for (int i = 0; i < counts; i++) {
                getMailContent(multipart.getBodyPart(i));
            }
        } else if (part.isMimeType("message/rfc822")) {
            getMailContent((Part) part.getContent());
        } else {
        }

        return bodytext.toString();
    }

    private static Map<String, String> extractCodeAndMoney(String content) {
        if (null == content || 0 == content.length()) {
            return null;
        }

        Map<String, String> ret = new HashMap<String, String>();
        int end = content.indexOf("$");
        int begin = content.indexOf("Claim code");
        String code = content.substring(begin + 11, begin + 28);
        String money = content.substring(end, end + 5);

        // System.out.println(code + " --> " + code.trim());
        // System.out.println(money + " --> " + money.trim());

        ret.put(code.trim(), money.trim());

        return ret;
    }

    private static void write2File(List<Map<String, String>> listMapCodeMoney) throws IOException {
        if (null == listMapCodeMoney || 0 == listMapCodeMoney.size()) {
            return;
        }

        final String path = "AmazonCode.csv";
        File file = new File(path);
        file.createNewFile();

        BufferedWriter output = new BufferedWriter(new FileWriter(file));

        int count = 1;
        for (Map<String, String> map : listMapCodeMoney) {
            Set<String> set = map.keySet();
            for (String code : set) {
                output.write(count + "," + code + "," + map.get(code));
                output.write("\r\n");
                count++;
            }
        }

        output.close();
        file = null;
    }
}