package com.james.temp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import com.james.common.util.JamesUtil;
import com.james.temp.SqlLineageUtil;
import com.james.temp.TableRelation;

public class HiveTableLineageParserTableLevel {
    private static ParseDriver pd = new ParseDriver();

    public static void main(String[] args) throws IOException {
        // ParseDriver pd = new ParseDriver();

        String sqlDemo = "INSERT INTO TABLE f_tt SELECT at_a.a_a AS f_a_a, at_b.b_b AS f_b_b, at_b.b_c AS f_b_c FROM(SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b, MAX(a_c) AS a_c FROM t_a WHERE a_c = 3 GROUP BY a_key ORDER BY a_a) at_a LEFT JOIN (SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b, MAX(b_c) AS b_c FROM t_b GROUP BY b_key ORDER BY b_b) at_b ON at_a.a_key = at_b.b_key";
        String sqlDemo_1 = "INSERT INTO TABLE f_tt SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b, MAX(a_c) AS a_c FROM t_a WHERE a_c = 3 GROUP BY a_key ORDER BY a_a";

        String sql52 = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_a.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
        String sql52_b = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_b.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
        String sql71 = "INSERT INTO TABLE sng_mp_etldata.t_kandian_tuwen_tuji_hourly SELECT * from(SELECT 2019030919 AS tdbank_imp_date, md5(A.uin,'kd_uin') AS uin, A.rowkey, B.ex_id, A.op_type, A.time,A.os,A.imei,A.idfa,A.imsi,A.op_cnt, 0 AS pic_cnt, 2 AS TYPE FROM (SELECT uin,op_type, get_json_object(d4,'$.rowkey') AS rowkey, regexp_replace(reporttime,'[^0-9]','') AS time, get_json_object(d4,'$.os') AS os, get_json_object(d4,'$.imei') AS imei, get_json_object(d4,'$.idfa') AS idfa, get_json_object(d4,'$.imsi') AS imsi, op_cnt FROM hlw.t_dw_dc01160 WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X8007625','0X8007626') AND substr(get_json_object(d4,'$.rowkey'),15,16) IN ('26','50','51','52','53','54','55','56','57','58','59') UNION ALL SELECT md5(uin,'kd_uin') AS uin,op_type, get_json_object(extra_info,'$.rowkey') AS rowkey, time, get_json_object(extra_info,'$.os') AS os, get_json_object(extra_info,'$.imei') AS imei, get_json_object(extra_info,'$.idfa') AS idfa, get_json_object(extra_info,'$.imsi') AS imsi, 1 AS op_cnt FROM sng_mp_etldata.t_mp_article_click_table_hourly WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X800662D','0X800662E') AND substr(get_json_object(extra_info,'$.rowkey'),15,16) IN ('26','50','51','52','53','54','55','56','57','58','59')) A LEFT JOIN (SELECT rowkey,ex_id FROM sng_mp_etldata.t_article_hbase_hourly_limit WHERE src IN ('26','49','50','51','52','53','54','55','56','57','58','59') AND imp_date = 2019030919) B ON A.rowkey = B.rowkey UNION ALL SELECT 2019030919 AS tdbank_imp_date, C.uin, C.rowkey, D.ex_id, C.op_type, C.time, NULL AS os, C.imei, C.idfa, C.imsi, C.op_cnt, C.pic_cnt, 1 AS TYPE FROM (SELECT md5('',uin) AS uin, op_type, get_json_object(d4,'$.rowkey') AS rowkey, reporttime AS time, imei, get_json_object(d4,'$.idfa') AS idfa, get_json_object(d4,'$.imsi') AS imsi, op_cnt, CASE WHEN op_type = '0X8008E30' THEN size(split(get_json_object(d4,'$.one_pic_reported'),'\\\\\\}\\\\\\,\\\\\\{')) ELSE 0 END AS pic_cnt FROM hlw.t_dw_dc01160 WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X8007625', '0X8007626', '0X8008E30') AND substr(get_json_object(d4,'$.rowkey'),15,2)='49')C JOIN (SELECT rowkey,ex_id FROM sng_mp_etldata.t_article_hbase_hourly_limit WHERE src = 49 AND sub_src = '490002' AND imp_date = 2019030919)D ON C.rowkey = D.rowkey) t_outer";

        // TODO
        String sql81 = "INSERT INTO TABLE t_kandian_account_video_uv_daily_new SELECT 20190226, 'aaaaa' AS s_a, C.puin puin , C.row_key , CASE WHEN SOURCE IN('1' ,'3') THEN 1 ELSE 0 END AS is_kd_source , CASE WHEN SOURCE='hello' THEN 1 ELSE 0 END AS s_kd_source , uv, vv a_vv, c.uv c_uv, d.puin d_puin FROM(SELECT puin , A.row_key , COUNT(DISTINCT A.cuin) AS uv , SUM(A.vv) AS vv FROM (SELECT case when rowkey=\"AAA\" then 'yes' else 'no' end rettt,cuin , business_id AS puin , op_cnt AS vv , rowkey AS row_key , RANK() OVER (PARTITION BY rowkey ORDER BY ftime) AS f_rank FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate = 20190226 AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ('ab' , 'ae' , 'af' , 'aj' , 'al' , 'ao') AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100) A LEFT JOIN (SELECT MAX(fdate) AS tdbank_imp_date , rowkey AS row_key , SUM(op_cnt) AS history_vv FROM sng_cp_fact.v_ty_BBBB WHERE fdate BETWEEN DATE_SUB(20190226, 90) AND DATE_SUB(20190226, 1) AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ('ab' , 'ae' , 'af' , 'aj' , 'al' , 'ao') AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 GROUP BY rowkey) B ON A.row_key = B.row_key WHERE ((B.history_vv IS NOT NULL AND f_rank < (3000001 - B.history_vv)) OR (f_rank < 3000001 AND B.history_vv IS NULL)) GROUP BY A.puin , A.row_key) C LEFT JOIN (SELECT puin , row_key , CASE WHEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') IS NOT NULL THEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') ELSE GET_JSON_OBJECT(MAX(extra_info), '$.src') END AS SOURCE FROM sng_tdbank . cc_dsl_content_center_rpt_fdt0 WHERE tdbank_imp_date BETWEEN DATE_SUB(20190226, 90) AND 20190226 AND op_type = '0XCC0V000' AND GET_JSON_OBJECT(extra_info, '$.renewal') NOT IN ('1') AND src IN ('2' , '5' , '6' , '10' , '12' , '15') GROUP BY puin , row_key) D ON C.row_key = D.row_key";
        String sql82 = "INSERT INTO TABLE sng_mp_etldata.t_kandian_video_uv_ext_daily_new SELECT f_date,puin,A.row_key,is_kd_source,kd_chann,kd_sec_chann,kd_trd_chann,teg_tag,video_time,uv,vv FROM(SELECT * FROM sng_mp_etldata.t_kandian_account_video_uv_daily_new WHERE f_date = 20190310) A JOIN ( SELECT row_key,kd_chann,kd_sec_chann,kd_trd_chann,kd_tag as teg_tag,GET_JSON_OBJECT(extra_info, '$.video_time') AS video_time FROM sng_mp_etldata.t_cc_inuse_content_center_720day WHERE f_date=20190310 and tdbank_imp_date BETWEEN DATE_SUB(20190310, 90) AND 20190310 AND SUBSTR(row_key, 15, 2) IN ( 'ab','ae','af','aj' ,'al' ,'ao' ) ) B ON A.row_key = B.row_key";
        String sql83 = "INSERT INTO TABLE sng_mp_etldata.t_kandian_account_article_uv_ext_daily SELECT f_date,puin,A.row_key,teg_chann,teg_sec_chann,teg_tag,uv,pv FROM(SELECT * FROM sng_mp_etldata.t_kandian_account_article_uv_daily WHERE f_date = 20190311) A JOIN ( SELECT row_key,MAX(puin) AS f_puin,GET_JSON_OBJECT(MAX(extra_info), '$.teg_chann') AS teg_chann,GET_JSON_OBJECT(MAX(extra_info), '$.teg_sec_chann') AS teg_sec_chann ,GET_JSON_OBJECT(MAX(extra_info), '$.teg_tag') AS teg_tag FROM sng_tdbank . cc_dsl_content_center_rpt_fdt0 WHERE op_type = '0XCC0A001' AND tdbank_imp_date BETWEEN DATE_SUB(20190311, 6) AND 20190311 GROUP BY row_key ) B ON A.row_key = B.row_key";
        String sql84 = "INSERT INTO TABLE sng_mp_etldata.t_kandian_account_article_uv_daily SELECT 20190311 as f_date,puin,A.row_key as row_key,COUNT(DISTINCT A.uin) AS uv,SUM(A.pv) AS pv FROM( SELECT uin,puin,GET_JSON_OBJECT(extra_info, '$.rowkey') AS row_key ,CASE WHEN op_type = '0X8006AC0' THEN CAST(GET_JSON_OBJECT(extra_info, '$.stayTime') AS INT) WHEN op_type = '0X800898E' THEN ROUND(GET_JSON_OBJECT(extra_info, '$.read_time')) ELSE 0 END AS stayTime_s,1 AS pv,time ,RANK() OVER (PARTITION BY GET_JSON_OBJECT(extra_info, '$.rowkey') ORDER BY time) AS f_rank FROM sng_mp_etldata . t_mp_article_click_table_hourly WHERE tdbank_imp_date BETWEEN 2019031100 AND 2019031123 AND op_type IN ('0X8006AC0','0X800898E') AND uin > 0 AND puin > 100 AND GET_JSON_OBJECT(extra_info, '$.rowkey') IS NOT NULL AND LENGTH(GET_JSON_OBJECT(extra_info, '$.rowkey')) = 16 AND SUBSTR(GET_JSON_OBJECT(extra_info, '$.rowkey'), 15, 2) IN ('02','07','28','42')) A left JOIN ( SELECT row_key,SUM(case when op_type = '0X8005899' then pv else 0 end) AS pv, case when round(sum(case when op_type = '0X8006AC0_pagePixel' then pv else 0 end)/sum(case when op_type = '0X8006AC0_pagePixel' then uv else 0 end)) < 1200 then 1 else 0 end as is_short FROM sng_mp_etldata.cc_dsl_article_exp_click_ellen WHERE f_date BETWEEN DATE_SUB(20190311, 7) AND DATE_SUB(20190311, 1) AND op_type IN ('0X8005899','0X8006AC0_pagePixel') AND src in ('02','07','28','42') GROUP BY row_key ) B ON A.row_key = B.row_key where ((B.pv is not NULL and f_rank < (500001 - B.pv) ) or (f_rank < 500001 and B.pv is NULL)) and (A.stayTime_s > 10 or (B.is_short = 0 AND A.stayTime_s > 5)) AND A.stayTime_s < 3600 group by puin, A.row_key ";
        String sql85 = "INSERT INTO TABLE sng_mp_etldata.t_kandian_account_income_detail_daily_new select 20190310 AS f_date,A.puin as puin, C.f_name, CASE WHEN B.f_credit_score IS NULL THEN 100 ELSE B.f_credit_score END AS credit_score, case when B.f_credit_score is null then 1.0 when B.f_credit_score between 70 and 100 then B.f_credit_score/100 when B.f_credit_score between 60 and 69 then 0.5 when B.f_credit_score between 30 and 59 then 0.2 when B.f_credit_score between 0 and 29 then 0.1 end as credit_score_weight, D.level AS account_level, CASE WHEN F.article_cpm is not null and F.article_cpm >= 0 then F.article_cpm else G.article_cpm end as article_cpm, CASE WHEN F.video_cpm is not null and F.video_cpm >= 0 then F.video_cpm else G.video_cpm end as video_cpm, case when Y.article_uv is not null then Y.article_uv else 0 end, case when X.video_vv is not null then X.video_vv else 0 end, case when X.kd_source_uv is not null then X.kd_source_uv else 0 end,(case when Y.article_uv is not null then Y.article_uv else 0 end * CASE WHEN F.article_cpm is not null and F.article_cpm >= 0 then F.article_cpm else G.article_cpm end / 1000.0), (case when X.video_vv is not null then X.video_vv else 0 end * CASE WHEN F.video_cpm is not null and F.video_cpm >= 0 then F.video_cpm else G.video_cpm end / 1000.0), (case when X.kd_source_uv is not null then X.kd_source_uv else 0 end * CASE WHEN F.video_cpm is not null and F.video_cpm >= 0 then F.video_cpm else G.video_cpm end * 0.1 / 1000.0), (case when Y.article_uv is not null then Y.article_uv else 0 end * CASE WHEN F.article_cpm is not null and F.article_cpm >= 0 then F.article_cpm else G.article_cpm end + (case when X.video_vv is not null then X.video_vv else 0 end + case when X.kd_source_uv is not null then X.kd_source_uv else 0 end * 0.1) * CASE WHEN F.video_cpm is not null and F.video_cpm >= 0 then F.video_cpm else G.video_cpm end +case when T.tuji_uv is not null then T.tuji_uv else 0 end * CASE WHEN F.picture_cpm is not null and F.picture_cpm >= 0 then F.picture_cpm else (CASE WHEN G.picture_cpm is not null and G.picture_cpm >=0 THEN G.picture_cpm ELSE 0.0 end) end +(case when Z.ans_uv is not null then Z.ans_uv else 0 end * CASE WHEN F.question_cpm is not null and F.question_cpm >= 0 then F.question_cpm else (CASE WHEN G.question_cpm is not null and G.question_cpm >=0 THEN G.question_cpm ELSE 0.0 end) end)+(case when min_income is not null and min_income>0 and ans_rowkey_num>=20 then min_income else 0.0 end*1000)) * (case when B.f_credit_score is null then 1.0 when B.f_credit_score between 70 and 100 then B.f_credit_score/100 when B.f_credit_score between 60 and 69 then 0.5 when B.f_credit_score between 30 and 59 then 0.2 when B.f_credit_score between 0 and 29 then 0.1 end) / 1000.0, case when T.tuji_uv is not null then T.tuji_uv else 0 end, (case when T.tuji_uv is not null then T.tuji_uv else 0 end * CASE WHEN F.picture_cpm is not null and F.picture_cpm >= 0 then F.picture_cpm else G.picture_cpm end / 1000.0), CASE WHEN F.picture_cpm is not null and F.picture_cpm >= 0 then F.picture_cpm else G.picture_cpm end AS tuji_cpm, case when Z.ans_uv is not null then Z.ans_uv else 0 end as ans_uv, (case when Z.ans_uv is not null then Z.ans_uv else 0 end * CASE WHEN F.question_cpm is not null and F.question_cpm >= 0 then F.question_cpm else G.question_cpm end / 1000.0)+(case when min_income is not null and min_income>0 and ans_rowkey_num>=20 then min_income else 0.0 end) as ans_income, CASE WHEN F.question_cpm is not null and F.question_cpm>= 0 then F.question_cpm else G.question_cpm end as question_cpm from ( SELECT puin FROM sng_mp_etldata.t_kandian_flow_master_daily WHERE f_date = 20190310 ) A LEFT JOIN ( SELECT puin, max(f_credit_score) as f_credit_score FROM sng_publicaccount_msgclickreport . tdw_media_info WHERE f_stat_day = 20190310 AND puin > 0 group by puin ) B ON A.puin = B.puin JOIN ( SELECT f_uin, f_name FROM sng_mp_cdbdata . t_pub_account_daily ) C ON C.f_uin = A.puin LEFT JOIN ( select M.puin as puin, case when N.level is not null then N.level else 2 end as level from (SELECT puin FROM sng_mp_etldata.t_kandian_flow_master_daily WHERE f_date = 20190310) M left join (SELECT puin, level FROM sng_mp_etldata . rich_puin_level WHERE f_date = 20190310) N on M.puin = N.puin ) D ON A.puin = D.puin left JOIN ( SELECT uin, cpm_article AS article_cpm, cpm_video AS video_cpm, cpm_picture AS picture_cpm, cpm_question AS question_cpm FROM sng_mp_etldata . t_account_cpm_cdb WHERE stat_day = 20190310 ) F ON A.puin = F.uin left JOIN ( SELECT LEVEL, cpm_article AS article_cpm, cpm_video AS video_cpm, cpm_picture AS picture_cpm, cpm_question AS question_cpm FROM sng_mp_etldata . t_level_cpm_cdb WHERE stat_day = 20190310 ) G ON D.Level = G.LEVEL left join ( SELECT puin, SUM(uv) AS video_vv, SUM(CASE WHEN is_kd_source = 1 THEN uv ELSE 0 END) AS kd_source_uv FROM sng_mp_etldata.t_kandian_video_uv_ext_daily_new WHERE f_date = 20190310 GROUP BY puin ) X on A.puin = X.puin left JOIN ( SELECT puin, SUM(uv) AS article_uv FROM sng_mp_etldata.t_kandian_account_article_uv_ext_daily WHERE f_date = 20190310 GROUP BY puin ) Y ON A.puin = Y.puin left JOIN ( SELECT puin, SUM(uv) AS tuji_uv FROM sng_mp_etldata.t_kandian_account_tuji_uv_daily WHERE f_date = 20190310 GROUP BY puin ) T ON A.puin = T.puin left join( select a.puin,ans_pv,ans_uv,min_income,ans_rowkey_num from (select puin,count(distinct ans_rowkey) as ans_rowkey_num from sng_mediaaccount_app.wenda_jiesuan_flow where substr(ftime,1,6)=substr(20190310,1,6) GROUP by puin) a left join (select puin,sum(ans_pv) as ans_pv,sum(ans_uv) as ans_uv from sng_mp_etldata.account_wenda_uv_daily where f_date=20190310 group by puin) b on a.puin=b.puin left join (select puin,min_income from sng_mp_etldata.kd_wenda_minimum_guarantee where imp_date=20190310) c on a.puin=c.puin ) Z on A.puin=Z.puin";

        String parsesql = sqlDemo;
        System.out.println(parsesql);

        ASTNode ast = null;
        try {
            ast = pd.parse(parsesql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(ast.toStringTree());
        JamesUtil.printDivider();

        // parseTableLineage
        parseTableLevelLineage(ast);
    }

    public static ASTNode getFromTok(ASTNode ast) {
        if (null != ast.getChildren() || 0 != ast.getChild(0).getChildCount()) {
            return (ASTNode) ast.getChild(0).getChild(0);
        }

        return null;
    }

    public static void parseTableLevelLineage(ASTNode ast) {
        if (null == ast || 0 == ast.getChildCount()) {
            return;
        }

        System.out.println("parseTableLineage(ASTNode ast)");

        ASTNode astFrom = getFromTok(ast);

        System.out.println(astFrom.getType() == HiveParser.TOK_FROM);

        switch (astFrom.getChild(0).getType()) {

        case HiveParser.TOK_LEFTOUTERJOIN:
        case HiveParser.TOK_RIGHTOUTERJOIN:
        case HiveParser.TOK_JOIN:
            System.out.println("HiveParser.TOK_JOIN:");
            break;
        case HiveParser.TOK_TABREF:
            System.out.println("HiveParser.TOK_TABREF:");
            break;
        default:
            break;
        }
    }
}