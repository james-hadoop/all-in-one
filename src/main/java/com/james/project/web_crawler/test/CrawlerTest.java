package com.james.project.web_crawler.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class CrawlerTest {

    public static void main(String[] args) throws ClientProtocolException, IOException, JSONException {
        String url = "http://12366.cqsw.gov.cn:6001/essearch/api/search?keyword=%E4%BC%81%E4%B8%9A%E6%89%80%E5%BE%97%E7%A8%8E&keyWordsRange=titleOrContent&kssj=%22%22&jssj=%22%22&sort=%22%22&currentPage=1";

        // String httpContent = getHttpContent(url);
        // System.out.println("httpContent:\n" + httpContent);

        // List<String> urlList = getUrls("");
        // System.out.println("urlList.size()=" + urlList.size());

        // List<String> percentageList = extractPercentage("");
        // printList(percentageList);

        Map<String, String> parsedContentMap = processContent("", "企业所得税");
        printMap(parsedContentMap);
    }

    private static String getHttpContent(String url) throws ClientProtocolException, IOException {
        if (null == url || url.isEmpty() || !url.contains("http")) {
            return null;
        }

        HttpGet get = new HttpGet(url);

        HttpClient client = HttpClients.createDefault();

        HttpResponse response = client.execute(get);
        HttpEntity entity = response.getEntity();
        String httpContent = EntityUtils.toString(entity, "UTF-8");

        return httpContent;
    }

    private static List<String> getUrls(String httpContent) throws JSONException {
        httpContent = "{\"success\":true,\"data\":[{\"docid\":172444,\"doctitle\":\"<b>姚朝智</b>：市局党组成员、副局长\",\"doccontent\":\"，2015年<span style='color:red'>9</span>月至今任重庆市国家税务局党组成员、副局长。\\n　\\n \\n分管工作重要活动\\n分管办公室、<span style='color:red'>所得税</span>处、国际税务管理处、财务管理处。◆学习十九大精神\\n　　 \\n 个人简历\\n \\n\\n　 \\n    姚朝智，男，汉族，1965年<span style='color:red'>12</span>月生，重庆市人，1989年7月参加工作，1988年5月加入中国共产党，全日制大学本科学历。现任重庆市国家税务局党组成员、副局长国家税务局副局长（其间，2000年11月至2001年<span style='color:red'>12</span>月在大连市国家税务局西岗分局挂职党组成员、副局长），2002年至2003年任重庆市万盛区国家税务局党组副书记、副局长(主持工作)，2003年至庆市国家税务局办公室主任，2009年至2010年任重庆市经开区国家税务局党组副书记、局长，2010年至2011年任重庆市北部新区国家税务局党组书记、局长，2011年<span style='color:red'>9</span>月任重庆市国家税务局党组成员、总会计师\",\"docpuburl\":\"http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_xxgk/Xbb_xxgkLdzl/201608/t20160819_172444.html\",\"docpubtime\":1516054926000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":194046,\"doctitle\":\"国家税务总局关于执行税收协定教师和研究人员条款有关问题的公告\",\"doccontent\":\"和研究人员征免个人<span style='color:red'>所得税</span>问题的通知》(〔86〕财税协字第030号)的规定计算。\\n　　五、来自缔约对方的税收居民需要享受税收协定教师和研究人员条款规定待遇的，应按照《国家税务总局关于印发〈非居民享受税收协定待遇管理办法(试行)〉的通知》(国税发〔2009〕124号)及其相关规定办理备案报告手续。不能适用税收协定教师和研究人员条款规定的<span style='color:red'>所得</span>，仍可以按照有关规定适用税收协定其他条款(如独立个人劳务条款、非独\",\"docpuburl\":\"http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_bsfw/Xbb_bsfwBsfd/Xbb_bsfwSsxd/201608/t20160824_194046.html\",\"docpubtime\":1515982653000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":194048,\"doctitle\":\"国家税务总局关于税收协定常设机构认定等有关问题的通知\",\"doccontent\":\"的。\\n　　三、对于缔约国对方居民个人为常设机构工作取得的工资、薪金<span style='color:red'>所得</span>，应按照税收协定“非独立个人劳务”（或“受雇<span style='color:red'>所得</span>”）条款和相关国内税法的规定，计算征收个人<span style='color:red'>所得税</span>。对于涉及为缔约国对方政府提供服务的，按照税收各省、自治区、直辖市和计划单列市国家税务局、地方税务局，扬州税务进修学院：\\n　　我国对外谈签的税收协定第五条（常设机构）第一款规定：“常设机构”一语是指<span style='color:red'>企业</span>进行全部或部分营业的固定场所；第四款规定：应认为，常设机构不包括专门为本<span style='color:red'>企业</span>进行准备性或辅助性活动的目的所设的固定营业场所。根据联合国税收协定范本注释、经济合作发展组织税收协定范本注释以及世界上多数国家的做法，对“营业”一语和“准备性或辅助性\",\"docpuburl\":\"http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_bsfw/Xbb_bsfwBsfd/Xbb_bsfwSsxd/201608/t20160824_194048.html\",\"docpubtime\":1515982653000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":263092,\"doctitle\":\"区国税局 适应经济新常态 谋求税务新发展\",\"doccontent\":\"税收优惠政策。落实西部大开发税收优惠86户，减免税额1.5亿元；落实研发费用加计扣除政策15户次，优惠金额9225万元；享受农林牧渔业项目<span style='color:red'>所得</span>减免67户，<span style='color:red'>所得</span>减免1463万元；享受小型微利<span style='color:red'>企业所得税</span>优惠撰写心得体会。目前，13个党支部开展以十九大精神为主题的集中学习39次，支部与支部之间、支部与<span style='color:red'>企业</span>之间开展以学习十九大为主题的党日活动10次，不断将十九大报告精神学习引向深入。\\n    认真抓好党的台账，从源头上进行管理规范。进一步完善党务公开制度，发布各类党务动态信息30余条。\\n    创新组织活动形式，局机关党委与市国税局处室支部、机关支部与税务所支部、区局党组织与<span style='color:red'>企业</span>党组织、与农村基层人面前的一道难题。\\n    谋大略者，以智取胜，在这场攻坚战中，坚定信心，砥砺奋进是打赢攻坚战的关键。\\n    区国税局结合经济发展新常态与税收政策调整新变化客观情况，广泛深入基层、<span style='color:red'>企业</span>开展税收调研。加强与各部门涉税信息共享，准确掌握各重点行业、重点<span style='color:red'>企业</span>、重点项目税源底数，把税收分析作为组织收入的重要抓手，加强分析预测和预判预警，牢牢把握组织收入主动权。\\n    在组织收入工作中，全局上下齐心协力，领导班子\",\"docpuburl\":\"http://www.cqsw.gov.cn/cqgszz/jjq/gzdt_989/201801/t20180115_263092.html\",\"docpubtime\":1515980042000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":263075,\"doctitle\":\"高新技术企业所得税优惠应当如何备案？\",\"doccontent\":\"　　问：高新技术<span style='color:red'>企业所得税</span>优惠应当如何备案？\\n　　答：一、高新技术<span style='color:red'>企业</span>享受减按15%税率征收<span style='color:red'>企业所得税</span>备案要求\\n　　根据《北京市国家税务局关于纳税人经认定后享受<span style='color:red'>企业所得税</span>税收优惠有关事项的公告》（北京国税公告2015年第6号 ）的规定，经认定的高新技术<span style='color:red'>企业</span>，享受高新技术<span style='color:red'>企业所得税</span>税收优惠时，可不再向主管税务机关报送备案资料，相关备案资料由纳税人妥善保存备查。\\n　　二、高新技术<span style='color:red'>企业</span>年度纳税申报报送资料要求\\n　　由于2014年度实行新的<span style='color:red'>企业所得税</span>年度纳税申报表，在新年度纳税申报表附表《高新技术<span style='color:red'>企业</span>优惠情况及明细表》中，已列示<span style='color:red'>企业</span>当年产品（服务）属于《国家重点支持的高新技术领域》规定的范围情况、高新技术产品（服务）收入占<span style='color:red'>企业</span>总收入的比例情况和人员占比情况，因此按照北京市国家税务局公告2015年5号所附《网上办理<span style='color:red'>企业所得税</span>涉税事项的资料清单》的规定，高新技术<span style='color:red'>企业</span>仅在年度申报时报送当年<span style='color:red'>企业</span>研究开发费用结构归集表。\\n \\n　　来源：国家税务总局微信\",\"docpuburl\":\"http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_gzcy/Xbb_gzcyRdwt/Xbb_gzxyZzs/201801/t20180115_263075.html\",\"docpubtime\":1515970664000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":263072,\"doctitle\":\"企业所得税年度纳税申报表修订！一图了解改哪儿了\",\"doccontent\":\"<span style='color:red'>企业所得税</span>年度纳税申报表修订！一图了解改哪儿了\",\"docpuburl\":\"http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_ssxc/Xbb_ssxcTjss/201801/t20180115_263072.html\",\"docpubtime\":1515970541000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":263063,\"doctitle\":\"梁平国地税局联合开展新办纳税人培训\",\"doccontent\":\"庆市电子税务局各个业务模块进行操作演示，同时通过分享“纳税信用‘换’真金白银”、“ 银税互动‘解’融资难题”的真实案例，对相关相关政策进行直观解读。地税局主要对个人<span style='color:red'>所得税</span>、房产税、印花税等地方税收政策做出\\n\\n<span style='color:red'>12</span>月22日，梁平国地税局联合举办新办纳税人培训会，全区共115户新办纳税人参加会议。\\n培训会上，县局相关科室人员重点对发票管理、涉税优惠、纳税申报、纳税服务等政策业务进行讲解，并在现场对重\",\"docpuburl\":\"http://www.cqsw.gov.cn/cqgszz/lpx/gzdt_755/201801/t20180115_263063.html\",\"docpubtime\":1515969005000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":23912,\"doctitle\":\"局情概况\",\"doccontent\":\"　　万盛经济技术开发区国家税务局位于重庆市万盛经开区塔山路3号（西区商务中心F栋），公开联系电话：023-48299099。主要负责辖区内增值税、消费税、<span style='color:red'>企业所得税</span>、储蓄利息个人<span style='color:red'>所得税</span>、车辆购置税；组织实施<span style='color:red'>企业所得税</span>、储蓄存款利息<span style='color:red'>所得税</span>和法律法规规定的基金（费）等征收管理工作，拟订征收管理具体实施办法；对有关税种的具体业务问题进行解释和处理；组织实施有关税种的纳税评估、税源管理、税基管理、汇算清缴等工作；指导有关税种的日常管理、日常检查；参与有关税种的纳税辅导、咨询服务、税收法律救济工作；承担大<span style='color:red'>企业</span>税收和国际税收管理工作。  \\n　　3.收入核算科  \\n　　牵头编制年度税收计划、出口退（免）税计划，分配下达年度免抵调库计划；监督检查税款缴、退库情况；开展税收收入分析预测、税收收入能力估算及重点税源监控、<span style='color:red'>企业</span>税收资料调查工作；承担税收会计核算、统计核算、税收票证管理等相关工作；承担税收收入数据\",\"docpuburl\":\"http://www.cqsw.gov.cn/cqgszz/wsjkq/jqgk_1382/201312/t20131212_23912.html\",\"docpubtime\":1515957745000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":263046,\"doctitle\":\"对纳税人变更纳税定额的核准办理规程\",\"doccontent\":\"\\n　　中华人民共和国税收征收管理法》第三十五条第一款\\n　　《中华人民共和国税收征收管理法实施细则》（中华人民共和国国务院令第 362 号）第四十七条\\n　　《国家税务总局 关于股权转让<span style='color:red'>所得</span>个人<span style='color:red'>所得税</span>条件为代理人代为办理的情形需提供。\\n　　8　办理流程\\n　　国税局办税服务厅（室）窗口咨询、申请→窗口受理初审→审核→复审→窗口电话通知申请人领取书面批文或决定（发放）→归档\\n　　<span style='color:red'>9</span>　审查类型、法规另有规定的，依照其规定。（实施税务行政许可的期限以工作日计算，不含法定节假日。）\\n　　<span style='color:red'>12</span>　收费依据及标准\\n　　无。\\n　　13　结果送达\\n　　窗口出件，准予税务行政许可决定书或不予税务行政许可\",\"docpuburl\":\"http://www.cqsw.gov.cn/cqgszz/wlx/gg_826/201801/t20180112_263046.html\",\"docpubtime\":1515716139000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null},{\"docid\":263045,\"doctitle\":\"对采取实际利润额预缴以外的其他企业所得税预缴方式的核定办理规程\",\"doccontent\":\"　　1　范围\\n　　本标准规定了对采取实际利润额预缴以外的其他<span style='color:red'>企业所得税</span>预缴方式的核定工作事项的办理依据、所需条件、申请材料、办理流程等。\\n　　本标准适用于县国税局窗口办理对采取实际利润额预缴以外的其他<span style='color:red'>企业所得税</span>预缴方式的核定工作事项。\\n　　2　办理依据\\n　　《中华人民共和国<span style='color:red'>企业所得税</span>法实施条例》（中华人民共和国国务院令第 512 号）\\n　　《国家税务总局关于公开行政审批事项等相关工作的公告（数量限制、禁止性要求）\\n　　根据《中华人民共和国<span style='color:red'>企业所得税</span>法实施条例》第 128 条，<span style='color:red'>企业所得税</span>分月或分季预缴，由税务机关具体核定。<span style='color:red'>企业</span>根据<span style='color:red'>企业所得税</span>法第 54 条规定分月或者分季预缴<span style='color:red'>企业所得税</span>时，应当按照月度或者季度的实际利润额预缴；按照月度或者季度的实际利润额预缴有困难的，可以按照上一纳税年度应纳税<span style='color:red'>所得</span>额的月度或者季度平均额预缴，或者按照经税务机关认可的其他方法预缴。\\n　　无数量限制。\\n　　7为代理人代为办理的情形需提供。\\n　　8　办理流程\\n　　国税局办税服务厅（室）窗口咨询、申请→窗口受理初审→审核→复审→窗口电话通知申请人领取书面批文或决定（发放）→归档\\n　　<span style='color:red'>9</span>　审查类型\",\"docpuburl\":\"http://www.cqsw.gov.cn/cqgszz/wlx/gg_826/201801/t20180112_263045.html\",\"docpubtime\":1515715834000,\"siteId\":null,\"operuser\":null,\"lm\":null,\"keyword\":null,\"title\":null,\"url\":null}],\"totalItem\":6326,\"pageSize\":10,\"currentPage\":1,\"totalPage\":633}\n"
                + "";
        System.out.println("httpContent:\n" + httpContent);

        if (null == httpContent || httpContent.isEmpty()) {
            return null;
        }

        List<String> urlList = new ArrayList<String>();

        JSONObject obj = new JSONObject(httpContent);
        String dataString = obj.getString("data");
        System.out.println("dataString:\n" + dataString);
        JSONArray data = new JSONArray(dataString);

        if (data == null || data.length() == 0) {
            return null;
        }

        for (int i = 0; i < data.length(); i++) {
            String urlString = (String) ((JSONObject) data.get(i)).get("docpuburl");
            System.out.println("urlString:\n\t" + urlString);

            urlList.add(urlString);
        }

        return urlList;
    }

    private static Map<String, String> processContent(String url, String keyword)
            throws ClientProtocolException, IOException {
        // url =
        // "http://www.cqsw.gov.cn/Xbb_gsgw/Xbb_xxgk/Xbb_xxgkLdzl/201608/t20160819_172444.html";
        // url="http://www.cqsw.gov.cn/cqgszz/wlx/gg_826/201801/t20180112_263045.html";
        url = "http://www.cqsw.gov.cn/cqgszz/ljxq/ssnews40/201705/t20170507_251231.html";

        if (null == url || url.isEmpty()) {
            return null;
        }

        String httpContent = getHttpContent(url);

        if (null == httpContent || httpContent.isEmpty() || null == keyword || keyword.isEmpty()
                || !httpContent.contains(keyword)) {
            return null;
        }

        Map<String, String> parsedContentMap = new HashMap<String, String>();
        parsedContentMap.put("keyword", keyword);
        parsedContentMap.put("url", url);

        if (httpContent.contains("%")) {
            List<String> percentageList = extractPercentage("");
            if (null == percentageList) {
                parsedContentMap.put("value", "");
            } else {
                StringBuilder sb = new StringBuilder();
                for (String percentage : percentageList) {
                    sb.append(percentage + ",");
                }

                parsedContentMap.put("value", sb.substring(0, sb.length() - 1).toString());
            }
        } else {
            parsedContentMap.put("value", "");
        }

        return parsedContentMap;
    }

    private static void printMap(Map<String, String> map) {
        if (null == map || map.isEmpty()) {
            System.out.println("parsedContentMap is null or empty.\n");
            return;
        }

        for (Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println("\n");
    }

    private static void printList(List<String> list) {
        if (null == list || list.isEmpty()) {
            System.out.println("list is null or empty.\n");
            return;
        }

        for (String e : list) {
            System.out.println(e);
        }
        System.out.println("\n");
    }

    private static List<String> extractPercentage(String content) {
        content = "企业负担不断减轻\n" + "\n" + "　　“前三季度的财政收支运行平稳，和当期经济形势相互协调，积极财政政策不断加力增效，有力促进了经济增长。”中国财政科学研究院副院长白景明说。\n"
                + "\n"
                + "　　在税收数据中，最引人关注的是营改增带来的减税效果。据统计，今年1月至9月份，国内增值税28014亿元，同比增长23.8%。主要是全面推开营改增试点后，原营业税纳税人改缴增值税形成收入转移且体现增收。其中，国内增值税（不含改征增值税）增长2.7%。营业税11405亿元，同比下降20.4%，主要是全面推开营改增试点后，原营业税纳税人改缴增值税，收入在增值税科目中反映，体现为增值税增收、营业税减收。\n"
                + "\n"
                + "　　“考虑收入在税种间转移因素，将改征增值税与营业税合并计算，前三季度累计增长11.4%，其中上半年增长24.2%，7、8、9月则分别下降10.9%、17.6%、21.3%，全面推开营改增试点的政策性减收效应逐步体现。”财政部有关负责人表示。\n"
                + "\n"
                + "　　据此前国家税务总局发布的统计数据，今年1月至7月份营改增整体减税共计2107亿元。“营改增不仅是我国税收制度的重要变革，同时承担起减轻企业税收负担，促进经济发展方式转变的历史重任。全面实施服务业营改增，使税制适应并促进了我国经济结构优化、产业层次提升、企业转型发展、服务贸易增强，为可持续发展提供了动力。”上海财经大学公共政策与治理研究院院长胡怡建说。\n"
                + "\n" + "　　房地产税收涨幅大\n" + "\n"
                + "　　除了营改增，前三季度税收收入的另一突出特点是房地产相关税收涨幅较大。数据显示，前三季度房地产企业所得税3035亿元，增长25.4%；个人所得税7903亿元，同比增长17%，其中，受二手房交易活跃等带动，财产转让所得税增长27.2%；契税3158亿元，同比增长11.7%；土地增值税3280亿元，同比增长13.7%。";

        if (null == content || content.isEmpty()) {
            return null;
        }

        List<String> percentageList = new ArrayList<String>();

        String patterString = "[0-9]+\\.[0-9]+%";
        Pattern pattern = Pattern.compile(patterString);
        Matcher matcher = pattern.matcher(content);

        while (matcher.find()) {
            percentageList.add(matcher.group());
        }

        return percentageList;
    }
}