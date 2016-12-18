package com.james.mail.amazon.parser.exported.driver;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeUtility;

import org.apache.commons.lang.StringUtils;

import com.james.mail.amazon.parser.conf.MailConf;
import com.james.mail.amazon.parser.driver.Util;
import com.james.mail.amazon.parser.entity.CardEntity;

public class ExportedCardParserDriver {
    static List<Map<String, CardEntity>> listMapCodeMoney = new ArrayList<Map<String, CardEntity>>();

    static String[] codeUpside;
    static String[] codeDownside;

    static Map<String, String> websiteToCountry = new HashMap<String, String>();

    public static void main(String args[]) throws Exception {
        init();

        Files.walkFileTree(Paths.get("cards"), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                try {
                    if (file.toFile().getAbsolutePath().endsWith(".eml")) {
                        parserFile(file.toFile().getAbsolutePath());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return super.visitFile(file, attrs);
            }
        });

        for (Map<String, CardEntity> map : listMapCodeMoney) {
            for (Entry<String, CardEntity> entry : map.entrySet()) {
                System.out.println(entry.getValue().getCode() + ": " + entry.getValue().getMoney() + " -> "
                        + entry.getValue().getUnit() + " -> " + entry.getValue().getCountry());
            }
        }
        
        Util.write2File(listMapCodeMoney);
    }

    private static void init() {
        codeUpside = new String(MailConf.CODE_UPSIDE).split(",");
        codeDownside = new String(MailConf.CODE_DOWNSIDE).split(",");

        for (String website : codeUpside) {
            websiteToCountry.put(website, website.substring(website.lastIndexOf("."), website.length()));
        }
        for (String website : codeDownside) {
            websiteToCountry.put(website, website.substring(website.lastIndexOf("."), website.length()));
        }
    }

    public static void parserFile(String emlPath) throws Exception {
        // System.out.println("emlPath=" + emlPath);
        Properties props = new Properties();
        Session session = Session.getDefaultInstance(props, null);
        InputStream inMsg;
        inMsg = new FileInputStream(emlPath);
        Message msg = new MimeMessage(session, inMsg);
        String mailContent = parseEml(msg);

        int index = mailContent.indexOf("Utilisation");
        if (-1 != index) {
            mailContent = mailContent.substring(0, index);
        }
        index = mailContent.indexOf("Inserisci il codice");
        if (-1 != index) {
            mailContent = mailContent.substring(0, index);
        }
        index = mailContent.indexOf("Utilización");
        if (-1 != index) {
            mailContent = mailContent.substring(0, index);
        }
        index = mailContent.indexOf("Redemption");
        if (-1 != index) {
            mailContent = mailContent.substring(0, index);
        }
        // System.out.println("mailContent=\n" + mailContent);

        String type = convertToStandardWebsite(mailContent, codeUpside, codeDownside);
        Map<String, CardEntity> codeToMoney = Util.extractCodeAndMoney(mailContent, type);
        listMapCodeMoney.add(codeToMoney);
    }

    private static String parseEml(Message msg) throws Exception {
        String mailContent = null;

        // 发件人信息
        Address[] froms = msg.getFrom();
        if (froms != null) {
            // System.out.println("发件人信息:" + froms[0]);
            InternetAddress addr = (InternetAddress) froms[0];
            // System.out.println("发件人地址:" + addr.getAddress());
            // System.out.println("发件人显示名:" + addr.getPersonal());
        }
        // System.out.println("邮件主题:" + msg.getSubject());
        // getContent() 是获取包裹内容, Part相当于外包装
        Object o = msg.getContent();
        if (o instanceof Multipart) {
            Multipart multipart = (Multipart) o;
            mailContent = reMultipart(multipart);
        } else if (o instanceof Part) {
            Part part = (Part) o;
            mailContent = rePart(part);
        } else {
            // System.out.println("类型" + msg.getContentType());
            // System.out.println("内容" + msg.getContent());
        }

        return mailContent;
    }

    /**
     * @param part
     *            解析内容
     * @throws Exception
     */
    private static String rePart(Part part) throws Exception {
        String ret = null;

        if (part.getDisposition() != null) {

            String strFileNmae = part.getFileName();
            if (!StringUtils.isEmpty(strFileNmae)) { // MimeUtility.decodeText解决附件名乱码问题
                strFileNmae = MimeUtility.decodeText(strFileNmae);
                // System.out.println("发现附件: " + strFileNmae);

                InputStream in = part.getInputStream();// 打开附件的输入流
                // 读取附件字节并存储到文件中
                java.io.FileOutputStream out = new FileOutputStream(strFileNmae);
                int data;
                while ((data = in.read()) != -1) {
                    out.write(data);
                }
                in.close();
                out.close();

            }

            // System.out.println("内容类型: " +
            // MimeUtility.decodeText(part.getContentType()));
            // System.out.println("附件内容:" + part.getContent());

        } else {
            if (part.getContentType().startsWith("text/plain")) {
                // System.out.println("文本内容：" + part.getContent());
                ret += (String) part.getContent();
            } else {
                // System.out.println("HTML内容：" + part.getContent());
            }
        }

        return ret;
    }

    /**
     * @param multipart
     *            // 接卸包裹（含所有邮件内容(包裹+正文+附件)）
     * @throws Exception
     */
    private static String reMultipart(Multipart multipart) throws Exception {
        String ret = null;

        // System.out.println("邮件共有" + multipart.getCount() + "部分组成");
        // 依次处理各个部分
        for (int j = 0, n = multipart.getCount(); j < n; j++) {
            // System.out.println("处理第" + j + "部分");
            Part part = multipart.getBodyPart(j);// 解包, 取出 MultiPart的各个部分,
                                                 // 每部分可能是邮件内容,
            // 也可能是另一个小包裹(MultipPart)
            // 判断此包裹内容是不是一个小包裹, 一般这一部分是 正文 Content-Type: multipart/alternative
            if (part.getContent() instanceof Multipart) {
                Multipart p = (Multipart) part.getContent();// 转成小包裹
                // 递归迭代
                ret += reMultipart(p);
            } else {
                ret += rePart(part);
            }
        }

        return ret;
    }

    private static String convertToStandardWebsite(String content, String[] codeUpside, String[] codeDownside) {
        for (String website : codeUpside) {
            if (content.contains(website)) {
                return websiteToCountry.get(website).substring(1, websiteToCountry.get(website).length());
            }
        }

        for (String website : codeDownside) {
            if (content.contains(website)) {
                return websiteToCountry.get(website).substring(1, websiteToCountry.get(website).length());
            }
        }

        return "";
    }
}