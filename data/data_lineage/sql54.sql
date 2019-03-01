INSERT INTO TABLE t_kandian_account_video_uv_daily_new
SELECT 20190226,
       'aaaaa' AS s_a,
       C.puin puin ,
       C.row_key ,
       CASE
           WHEN SOURCE IN('1' ,'3') THEN 1
           ELSE 0
       END AS is_kd_source ,
       CASE
           WHEN SOURCE='hello' THEN 1
           ELSE 0
       END AS s_kd_source ,
       uv,
       vv a_vv,
       c.uu c_uv
FROM
  (SELECT puin ,
          A.row_key ,
          COUNT(DISTINCT A.cuin) AS uv ,
          SUM(A.vv) AS vv
   FROM
     (SELECT cuin ,
             business_id AS puin ,
             op_cnt AS vv ,
             rowkey AS row_key ,
             RANK() OVER (PARTITION BY rowkey
                          ORDER BY ftime) AS f_rank
      FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean
      WHERE fdate = 20190226
        AND score < 80
        AND dis_platform = 1
        AND op_type = 3
        AND op_cnt > 0
        AND LENGTH(rowkey) = 16
        AND SUBSTR(rowkey, 15, 2) IN ('ab' ,
                                      'ae' ,
                                      'af' ,
                                      'aj' ,
                                      'al' ,
                                      'ao')
        AND play_time>0
        AND play_time/1000 BETWEEN 0 AND 3600
        AND video_length>0
        AND video_length/1000 BETWEEN 1 AND 7200
        AND ((play_time / video_length > 0.6
              AND video_length < 21000)
             OR (play_time > 10000
                 AND video_length > 20000))
        AND business_id > 100) A
   LEFT JOIN
     (SELECT MAX(fdate) AS tdbank_imp_date ,
             rowkey AS row_key ,
             SUM(op_cnt) AS history_vv
      FROM sng_cp_fact.v_ty_audit_all_video_play_basic_info_check_clean
      WHERE fdate BETWEEN DATE_SUB(20190226, 90) AND DATE_SUB(20190226, 1)
        AND score < 80
        AND dis_platform = 1
        AND op_type = 3
        AND op_cnt > 0
        AND LENGTH(rowkey) = 16
        AND SUBSTR(rowkey, 15, 2) IN ('ab' ,
                                      'ae' ,
                                      'af' ,
                                      'aj' ,
                                      'al' ,
                                      'ao')
        AND play_time>0
        AND play_time/1000 BETWEEN 0 AND 3600
        AND video_length>0
        AND video_length/1000 BETWEEN 1 AND 7200
        AND ((play_time / video_length > 0.6
              AND video_length < 21000)
             OR (play_time > 10000
                 AND video_length > 20000))
        AND business_id > 100
      GROUP BY rowkey) B ON A.row_key = B.row_key
   WHERE ((B.history_vv IS NOT NULL
           AND f_rank < (3000001 - B.history_vv))
          OR (f_rank < 3000001
              AND B.history_vv IS NULL))
   GROUP BY A.puin ,
            A.row_key) C
LEFT JOIN
  (SELECT puin ,
          row_key ,
          CASE
              WHEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type') IS NOT NULL THEN GET_JSON_OBJECT(MAX(extra_info), '$.store_type')
              ELSE GET_JSON_OBJECT(MAX(extra_info), '$.src')
          END AS SOURCE
   FROM sng_tdbank . cc_dsl_content_center_rpt_fdt0
   WHERE tdbank_imp_date BETWEEN DATE_SUB(20190226, 90) AND 20190226
     AND op_type = '0XCC0V000'
     AND GET_JSON_OBJECT(extra_info, '$.renewal') NOT IN ('1')
     AND src IN ('2' ,
                 '5' ,
                 '6' ,
                 '10' ,
                 '12' ,
                 '15')
   GROUP BY puin ,
            row_key) D ON C.row_key = D.row_key