package com.james.mail.amazon.parser.driver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.james.mail.amazon.parser.entity.CardEntity;

public class Util {
    public static void write2File(List<Map<String, CardEntity>> listMapCodeMoney) throws IOException {
        if (null == listMapCodeMoney || 0 == listMapCodeMoney.size()) {
            return;
        }

        final String path = "AmazonCode.csv";
        File file = new File(path);
        file.createNewFile();

        BufferedWriter output = new BufferedWriter(new FileWriter(file));

        output.write("id,code,money,unit,country");
        output.write("\r\n");

        int count = 1;
        for (Map<String, CardEntity> map : listMapCodeMoney) {
            for (Entry<String, CardEntity> entry : map.entrySet()) {

                output.write(count + "," + entry.getValue().getCode() + "," + entry.getValue().getMoney() + ","
                        + entry.getValue().getUnit() + "," + entry.getValue().getCountry());
                output.write("\r\n");
                count++;
            }
        }

        output.close();
        file = null;
    }

    public static Map<String, CardEntity> extractCodeAndMoney(String mailContent, String type) {
        if (null == mailContent || 0 == mailContent.length()) {
            return null;
        }

        Map<String, CardEntity> ret = new HashMap<String, CardEntity>();

        String code = "";
        String money = "";

        if (type.equals("fr")) {
            int codeIndex = mailContent.indexOf("Code chèque-cadeau");
            int moneyIndex = mailContent.indexOf("euro");

            if (-1 != codeIndex) {
                code = mailContent.substring(codeIndex + 40, codeIndex + 80);
            }
            if (-1 != moneyIndex) {
                money = mailContent.substring(moneyIndex - 7, moneyIndex - 2);
            }

            // System.out.println("code--> " + code.trim());
            // System.out.println("money --> " + money.trim());
            // System.out.println("country --> " + type);

            ret.put(code.trim(), new CardEntity(code.trim(), money.trim().replace(",", "."), "€", type));
        } else if (type.equals("jp")) {
            int codeIndex = mailContent.indexOf("ギフト券番号");
            int moneyIndex = mailContent.lastIndexOf("￥");

            if (-1 != codeIndex) {
                code = mailContent.substring(codeIndex + 26, codeIndex + 60);
            }
            if (-1 != moneyIndex) {
                money = mailContent.substring(moneyIndex + 1, moneyIndex + 10);
            }

            // System.out.println("code--> " + code.trim());
            // System.out.println("money --> " + money.trim());
            // System.out.println("country --> " + type);

            ret.put(code.trim(), new CardEntity(code.trim(), money.trim().replace(",", "."), "￥", type));
        } else if (type.equals("com")) {
            int codeIndex = mailContent.indexOf("Claim Code");
            int moneyIndex = mailContent.indexOf("$");

            if (-1 != codeIndex) {
                code = mailContent.substring(codeIndex + 11, codeIndex + 28);
            }
            if (-1 != moneyIndex) {
                money = mailContent.substring(moneyIndex + 1, moneyIndex + 10);
            }

            // System.out.println("code--> " + code.trim());
            // System.out.println("money --> " + money.trim());
            // System.out.println("country --> " + type);

            ret.put(code.trim(), new CardEntity(code.trim(), money.trim().replace(",", "."), "$", type));
        } else if (type.equals("ca")) {
            int codeIndex = mailContent.indexOf("Claim Code");
            int moneyIndex = mailContent.indexOf("$");

            if (-1 != codeIndex) {
                code = mailContent.substring(codeIndex + 11, codeIndex + 28);
            }
            if (-1 != moneyIndex) {
                money = mailContent.substring(moneyIndex + 1, moneyIndex + 10);
            }

            // System.out.println("code--> " + code.trim());
            // System.out.println("money --> " + money.trim());
            // System.out.println("country --> " + type);

            ret.put(code.trim(), new CardEntity(code.trim(), money.trim().replace(",", "."), "$", type));
        } else if (type.equals("it")) {
            int codeIndex = mailContent.lastIndexOf("Codice");
            int moneyIndex = mailContent.lastIndexOf("euro");

            if (-1 != codeIndex) {
                code = mailContent.substring(codeIndex + 28, codeIndex + 80);
            }
            if (-1 != moneyIndex) {
                money = mailContent.substring(moneyIndex + 6, moneyIndex + 17);
            }

            // System.out.println("code--> " + code.trim());
            // System.out.println("money --> " + money.trim());
            // System.out.println("country --> " + type);

            ret.put(code.trim(), new CardEntity(code.trim(), money.trim().replace(",", "."), "€", type));
        } else if (type.equals("de")) {
            int codeIndex = mailContent.indexOf("Gutschein-Code");
            int moneyIndex = mailContent.indexOf("euro");

            if (-1 != codeIndex) {
                code = mailContent.substring(codeIndex + 36, codeIndex + 80);
            }
            if (-1 != moneyIndex) {
                money = mailContent.substring(moneyIndex - 7, moneyIndex - 2);
            }

            // System.out.println("code--> " + code.trim());
            // System.out.println("money --> " + money.trim());
            // System.out.println("country --> " + type);

            ret.put(code.trim(), new CardEntity(code.trim(), money.trim().replace(",", "."), "€", type));
        } else if (type.equals("es")) {
            int codeIndex = mailContent.indexOf("Código Cheque regalo");
            int moneyIndex = mailContent.indexOf("euro");

            if (-1 != codeIndex) {
                code = mailContent.substring(codeIndex + 21, codeIndex + 80);
            }
            if (-1 != moneyIndex) {
                money = mailContent.substring(moneyIndex - 7, moneyIndex - 2);
            }

            // System.out.println("code--> " + code.trim());
            // System.out.println("money --> " + money.trim());
            // System.out.println("country --> " + type);

            ret.put(code.trim(), new CardEntity(code.trim(), money.trim().replace(",", "."), "€", type));
        } else if (type.equals("uk")) {
            int codeIndex = mailContent.indexOf("Claim Code");
            int moneyIndex = mailContent.indexOf("euro");

            if (-1 != codeIndex) {
                code = mailContent.substring(codeIndex + 11, codeIndex + 28);
            }
            if (-1 != moneyIndex) {
                money = mailContent.substring(moneyIndex - 7, moneyIndex - 2);
            }

            // System.out.println("code--> " + code.trim());
            // System.out.println("money --> " + money.trim());
            // System.out.println("country --> " + type);

            ret.put(code.trim(), new CardEntity(code.trim(), money.trim().replace(",", "."), "€", type));
        }

        return ret;
    }
}
