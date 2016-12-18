package com.james.mail.amazon.parser.driver;

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
import com.james.common.util.JamesUtil;
import com.james.mail.amazon.parser.conf.MailConf;

/**
 * This parser is used to parse the Amazon gift cards in the 126 mailbox
 * 
 * @author james
 *
 */
public class MailDriver {
    static Map<String, String> websiteToCountry = new HashMap<String, String>();

    static String[] codeUpside;
    static String[] codeDownside;

    public static void main(String[] args) throws Exception {
        List<Map<String, String>> listMapCodeMoney = new ArrayList<Map<String, String>>();

        /*
         * code position mapping
         */
        codeUpside = new String(MailConf.CODE_UPSIDE).split(",");
        codeDownside = new String(MailConf.CODE_DOWNSIDE).split(",");

        for (String website : codeUpside) {
            websiteToCountry.put(website, website.substring(website.indexOf(".") + 1, website.length()));
        }
        for (String website : codeDownside) {
            websiteToCountry.put(website, website.substring(website.indexOf(".") + 1, website.length()));
        }

        /*
         * mailbox properties
         */
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
            List<Message> listLatest100Message = listMessage.subList(listMessage.size() - 1, listMessage.size());

            String strYesterday = DateUtil.getLastNDay(1);
            StringBuffer bodytext = new StringBuffer();
            for (Message message : listLatest100Message) {
                String strReceiveDate = DateUtil.DateToString(message.getSentDate());
                System.out.println(strYesterday + " Vs " + strReceiveDate);

                if (strYesterday.compareTo(strReceiveDate) > 0) {
                    continue;
                }

                if (message.getContent() instanceof Multipart) {
                    String fileName = message.getFileName();
                    // System.out.println(fileName);

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
                mailContent = mailContent.substring(0, 1024);
                System.out.println("mailContext: " + mailContent);

                if (!mailContent.contains("Amazon") || !mailContent.contains("amazon")) {
                    continue;
                }

                bodytext.delete(0, bodytext.length());

                Map<String, String> mapCodeMoney = extractCodeAndMoney(mailContent);
                if (null != mapCodeMoney && 0 < mapCodeMoney.size()) {
                    listMapCodeMoney.add(mapCodeMoney);
                }
            }

            if (null != listMapCodeMoney && 0 < listMapCodeMoney.size()) {
                write2File(listMapCodeMoney);
            }
            JamesUtil.printDivider();
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

        String type = convertToStandardWebsite(content, codeUpside, codeDownside);

        if ("fr" == type) {
            // int end = content.indexOf("Amount:");
            // int begin = content.indexOf("Claim code");

            int end = content.indexOf("euro");
            int begin = content.indexOf("Code chèque-cadeau");
            String code = content.substring(begin + 40, begin + 80);
            String money = content.substring(end - 7, end - 2);

            System.out.println("code--> " + code.trim());
            System.out.println("money --> " + money.trim());
            System.out.println("country --> " + type);

            ret.put(code.trim(), money.trim());
        } else {
            int end = content.indexOf("euro");
            int begin = content.indexOf("Code chèque-cadeau");
            String code = content.substring(begin + 40, begin + 80);
            String money = content.substring(end - 7, end - 2);

            System.out.println("code--> " + code.trim());
            System.out.println("money --> " + money.trim());
            System.out.println("country --> " + type);

            ret.put(code.trim(), money.trim());
        }

        return ret;
    }

    private static String convertToStandardWebsite(String content, String[] codeUpside, String[] codeDownside) {
        for (String website : codeUpside) {
            if (content.contains(website)) {
                return websiteToCountry.get(website);
            }
        }

        for (String website : codeDownside) {
            if (content.contains(website)) {
                return websiteToCountry.get(website);
            }
        }

        return "";
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