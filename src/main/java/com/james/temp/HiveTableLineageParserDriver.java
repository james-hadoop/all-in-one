package com.james.temp;

import java.io.IOException;

import com.james.common.util.JamesUtil;
import com.james.temp.SqlLineageUtil;
import com.james.temp.TableRelation;

public class HiveTableLineageParserDriver {
	public static void main(String[] args) throws IOException {
		String sqlDemo = "INSERT INTO TABLE f_tt SELECT at_a.a_a AS f_a_a, at_b.b_b AS f_b_b, at_b.b_c AS f_b_c FROM(SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b, MAX(a_c) AS a_c FROM t_a WHERE a_c = 3 GROUP BY a_key ORDER BY a_a) at_a LEFT JOIN (SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b, MAX(b_c) AS b_c FROM t_b GROUP BY b_key ORDER BY b_b) at_b ON at_a.a_key = at_b.b_key";
		String sql52 = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_a.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
		String sql52_b = "INSERT INTO TABLE t_target SELECT r_t_a.r_a_a AS f_a_a, r_t_b.r_b_b AS f_b_b, r_t_b.r_b_c AS f_b_c, r_t_b.same FROM( SELECT a_key, a_a r_a_a, a_b AS r_a_b, MAX(a_c) AS r_a_c, MAX(same) AS same FROM t_a WHERE a_a = 1 GROUP BY a_key, a_a, a_b ORDER BY a_a desc) r_t_a LEFT JOIN ( SELECT b_key, MAX(b_a) AS r_b_a, MAX(b_b) AS r_b_b, MAX(b_c) AS r_b_c, MAX(same) AS same FROM t_b GROUP BY b_key ORDER BY b_b) r_t_b ON r_t_a.a_key = r_t_b.b_key ";
		String sql61 = "INSERT INTO TABLE t_kandian_account_video_uv_daily_new SELECT 20190226, 'aaaaa' AS s_a, C.puin puin , C.row_key , CASE WHEN SOURCE IN('1' ,'3') THEN 1 ELSE 0 END AS is_kd_source , CASE WHEN SOURCE='hello' THEN 1 ELSE 0 END AS s_kd_source , uv, vv a_vv, c.uv c_uv, d.puin d_puin FROM(SELECT puin , A.row_key , COUNT(DISTINCT A.cuin) AS uv , SUM(A.vv) AS vv FROM (SELECT case when rowkey=\"AAA\" then 'yes' else 'no' end rettt,cuin , business_id AS puin , op_cnt AS vv , rowkey AS row_key , RANK() OVER (PARTITION BY rowkey ORDER BY ftime) AS f_rank FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean WHERE fdate = 20190226 AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ('ab' , 'ae' , 'af' , 'aj' , 'al' , 'ao') AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100) A LEFT JOIN (SELECT MAX(fdate) AS tdbank_imp_date , rowkey AS row_key , SUM(op_cnt) AS history_vv FROM sng_cp_fact.v_ty_BBBB WHERE fdate BETWEEN DATE_SUB(20190226, 90) AND DATE_SUB(20190226, 1) AND score < 80 AND dis_platform = 1 AND op_type = 3 AND op_cnt > 0 AND LENGTH(rowkey) = 16 AND SUBSTR(rowkey, 15, 2) IN ('ab' , 'ae' , 'af' , 'aj' , 'al' , 'ao') AND play_time>0 AND play_time/1000 BETWEEN 0 AND 3600 AND video_length>0 AND video_length/1000 BETWEEN 1 AND 7200 AND ((play_time / video_length > 0.6 AND video_length < 21000) OR (play_time > 10000 AND video_length > 20000)) AND business_id > 100 GROUP BY rowkey) B ON A.row_key = B.row_key WHERE ((B.history_vv IS NOT NULL AND f_rank < (3000001 - B.history_vv)) OR (f_rank < 3000001 AND B.history_vv IS NULL)) GROUP BY A.puin , A.row_key) C LEFT JOIN (SELECT puin , row_key , CASE WHEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') IS NOT NULL THEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') ELSE GET_JSON_OBJECT(MAX(extra_info), '$.src') END AS SOURCE FROM sng_tdbank . cc_dsl_content_center_rpt_fdt0 WHERE tdbank_imp_date BETWEEN DATE_SUB(20190226, 90) AND 20190226 AND op_type = '0XCC0V000' AND GET_JSON_OBJECT(extra_info, '$.renewal') NOT IN ('1') AND src IN ('2' , '5' , '6' , '10' , '12' , '15') GROUP BY puin , row_key) D ON C.row_key = D.row_key";
		String sql71 = "INSERT INTO TABLE sng_mp_etldata.t_kandian_tuwen_tuji_hourly SELECT * from(SELECT 2019030919 AS tdbank_imp_date, md5(A.uin,'kd_uin') AS uin, A.rowkey, B.ex_id, A.op_type, A.time,A.os,A.imei,A.idfa,A.imsi,A.op_cnt, 0 AS pic_cnt, 2 AS TYPE FROM (SELECT uin,op_type, get_json_object(d4,'$.rowkey') AS rowkey, regexp_replace(reporttime,'[^0-9]','') AS time, get_json_object(d4,'$.os') AS os, get_json_object(d4,'$.imei') AS imei, get_json_object(d4,'$.idfa') AS idfa, get_json_object(d4,'$.imsi') AS imsi, op_cnt FROM hlw.t_dw_dc01160 WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X8007625','0X8007626') AND substr(get_json_object(d4,'$.rowkey'),15,16) IN ('26','50','51','52','53','54','55','56','57','58','59') UNION ALL SELECT md5(uin,'kd_uin') AS uin,op_type, get_json_object(extra_info,'$.rowkey') AS rowkey, time, get_json_object(extra_info,'$.os') AS os, get_json_object(extra_info,'$.imei') AS imei, get_json_object(extra_info,'$.idfa') AS idfa, get_json_object(extra_info,'$.imsi') AS imsi, 1 AS op_cnt FROM sng_mp_etldata.t_mp_article_click_table_hourly WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X800662D','0X800662E') AND substr(get_json_object(extra_info,'$.rowkey'),15,16) IN ('26','50','51','52','53','54','55','56','57','58','59')) A LEFT JOIN (SELECT rowkey,ex_id FROM sng_mp_etldata.t_article_hbase_hourly_limit WHERE src IN ('26','49','50','51','52','53','54','55','56','57','58','59') AND imp_date = 2019030919) B ON A.rowkey = B.rowkey UNION ALL SELECT 2019030919 AS tdbank_imp_date, C.uin, C.rowkey, D.ex_id, C.op_type, C.time, NULL AS os, C.imei, C.idfa, C.imsi, C.op_cnt, C.pic_cnt, 1 AS TYPE FROM (SELECT md5('',uin) AS uin, op_type, get_json_object(d4,'$.rowkey') AS rowkey, reporttime AS time, imei, get_json_object(d4,'$.idfa') AS idfa, get_json_object(d4,'$.imsi') AS imsi, op_cnt, CASE WHEN op_type = '0X8008E30' THEN size(split(get_json_object(d4,'$.one_pic_reported'),'\\\\\\}\\\\\\,\\\\\\{')) ELSE 0 END AS pic_cnt FROM hlw.t_dw_dc01160 WHERE tdbank_imp_date = 2019030919 AND op_type IN ('0X8007625', '0X8007626', '0X8008E30') AND substr(get_json_object(d4,'$.rowkey'),15,2)='49')C JOIN (SELECT rowkey,ex_id FROM sng_mp_etldata.t_article_hbase_hourly_limit WHERE src = 49 AND sub_src = '490002' AND imp_date = 2019030919)D ON C.rowkey = D.rowkey) t_outer";

		String parsesql = sql61;
		System.out.println(parsesql);

		JamesUtil.printDivider();
		TableRelation tableRelation = HiveTableLineageParserClean.parse(parsesql);
		System.out.println(tableRelation);

		SqlLineageUtil.makeGexf(tableRelation);
	}
}
