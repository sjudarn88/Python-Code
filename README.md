###DEPOSIT DATA
SELECT FROM_UNIXTIME(CAST(f_updated_at/1000 AS BIGINT), 'yyyy-MM-dd') AS f_date
    ,f_currency
    ,COUNT(DISTINCT f_user_id)
    ,SUM(f_amount) 
FROM huobi_usa.ods_usa_bitex_t_deposit_virtual 
WHERE f_state IN (9,12,15) AND FROM_UNIXTIME(CAST(f_updated_at/1000 AS BIGINT), 'yyyy-MM')='2018-08'
GROUP BY FROM_UNIXTIME(CAST(f_updated_at/1000 AS BIGINT), 'yyyy-MM-dd'),f_currency;

###WITHDRAW DATA
SELECT COALESCE(a.f_date,b.f_date) AS f_date,
    COALESCE(a.f_currency,b.f_currency) AS f_currency,
    COALESCE(a.app_usercount,0) AS app_usercount,
    COALESCE(b.cus_usercount,0) AS cus_usercount,
    COALESCE(b.f_currenysum,0) AS f_currenysum
FROM 
    (SELECT FROM_UNIXTIME(CAST((f_created_at/1000) AS BIGINT),'yyyy-MM-dd') AS f_date
            ,f_currency
            ,COUNT(DISTINCT f_user_id) AS app_usercount 
        FROM huobi_usa.ods_usa_bitex_t_withdraw_virtual 
        WHERE FROM_UNIXTIME(CAST((f_created_at/1000) AS BIGINT),'yyyy-MM-dd')='2018-08-21' 
        GROUP BY FROM_UNIXTIME(CAST((f_created_at/1000) AS BIGINT),'yyyy-MM-dd'),f_currency) AS a
    FULL JOIN 
    (SELECT FROM_UNIXTIME(CAST((f_updated_at/1000) AS BIGINT),'yyyy-MM-dd') AS f_date
            ,f_currency
            ,COUNT(DISTINCT f_user_id) AS cus_usercount
            ,SUM(f_amount) AS f_currenysum 
        FROM huobi_usa.ods_usa_bitex_t_withdraw_virtual 
        WHERE f_state = 11 AND FROM_UNIXTIME(CAST((f_updated_at/1000) AS BIGINT),'yyyy-MM-dd')='2018-08-21' 
        GROUP BY FROM_UNIXTIME(CAST((f_updated_at/1000) AS BIGINT),'yyyy-MM-dd'),f_currency) AS b
    ON (a.f_date = b.f_date AND a.f_currency = b.f_currency)
    
###TRADE AMOUNT AND TRADE FEES
SELECT 
        FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd') AS f_date
        ,f_symbol
        ,SUM(f_filled_cash_amount) AS tex_amount
        ,SUM(CASE WHEN f_order_type IN (1,3,5) THEN (f_filled_fees * f_price) + f_face_filled_fees
                WHEN f_order_type IN (2,4,6) THEN f_filled_fees + (f_face_filled_fees * f_price) 
                ELSE 0 end) AS tex_fee
    FROM huobi_usa.ods_usa_bitex_t_match_result
    WHERE FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd') = '2018-08-21'
        AND f_filled_amount > 0 AND f_role = 2
    GROUP BY FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd'),f_symbol


###TRADE USER
SELECT f_date,f_symbol,COUNT(DISTINCT user_id) AS tex_usercount
    FROM (SELECT FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd') AS f_date,f_symbol,f_user_id AS user_id
                FROM huobi_usa.ods_usa_bitex_t_match_result
                WHERE  f_filled_amount > 0 AND f_role = 2
                GROUP BY FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd'),f_symbol,f_user_id
        UNION ALL
        SELECT FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd') AS f_date,f_symbol,f_face_user_id AS user_id
                FROM huobi_usa.ods_usa_bitex_t_match_result
                WHERE f_filled_amount > 0 AND f_role = 2
                GROUP BY FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd'),f_symbol,f_face_user_id
        ) a 
    GROUP BY f_date,f_symbol


###REGISTERED USER
SELECT FROM_UNIXTIME(CAST(gmt_created/1000 AS BIGINT), 'yyyy-MM-dd') AS f_date
        ,COUNT(*) AS reg_usercount
    FROM huobi_usa.ods_usa_uc_uc_user 
    WHERE FROM_UNIXTIME(CAST(gmt_created/1000 AS BIGINT), 'yyyy-MM-dd') = '2018-08-21' 
    GROUP BY FROM_UNIXTIME(CAST(gmt_created/1000 AS BIGINT), 'yyyy-MM-dd') 

###LOGIN USER
SELECT FROM_UNIXTIME(CAST(gmt_created/1000 AS BIGINT),'yyyy-MM-dd') AS f_date
        ,COUNT(DISTINCT user_id) AS login_usercount
    FROM huobi_usa.ods_usa_uc_uc_login_history 
    WHERE FROM_UNIXTIME(CAST(gmt_created/1000 AS BIGINT),'yyyy-MM-dd') = '2018-08-21'
    GROUP BY FROM_UNIXTIME(CAST (gmt_created/1000 AS BIGINT),'yyyy-MM-dd') 
 
 ###L1 USER
 SELECT FROM_UNIXTIME(CAST(u1_auth_time/1000 AS BIGINT),'yyyy-MM-dd') AS f_date, COUNT(1) AS auth1_usercount
        FROM huobi_usa.ods_usa_lus_t_user_base 
    WHERE auth_state=1 AND auth_level = 1 AND FROM_UNIXTIME (CAST(u1_auth_time/1000 AS BIGINT),'yyyy-MM-dd') = '2018-08-21'
    GROUP BY FROM_UNIXTIME(CAST(u1_auth_time/1000 AS BIGINT),'yyyy-MM-dd')
    
 ###L2 USER
 SELECT FROM_UNIXTIME(CAST(u2_auth_time/1000 AS BIGINT),'yyyy-MM-dd') AS f_date, COUNT(1) AS auth1_usercount
        FROM huobi_usa.ods_usa_lus_t_user_base 
    WHERE auth_state=1 AND auth_level = 2 AND FROM_UNIXTIME (CAST(u2_auth_time/1000 AS BIGINT),'yyyy-MM-dd') = '2018-08-21'
    GROUP BY FROM_UNIXTIME(CAST(u2_auth_time/1000 AS BIGINT),'yyyy-MM-dd')
    
 ###L3 USER
SELECT FROM_UNIXTIME(CAST(u3_expire_time/1000 AS BIGINT) - 31536000, 'yyyy-MM-dd') AS f_date, COUNT(1) AS auth3_usercount
    FROM huobi_usa.ods_usa_lus_t_user_base 
    WHERE auth_level=3 AND auth_state=1 AND FROM_UNIXTIME(CAST(u3_expire_time/1000 AS BIGINT) - 31536000, 'yyyy-MM-dd') = '2018-08-21'
    FROM FROM_UNIXTIME(CAST (u3_expire_time/1000 AS BIGINT) - 31536000, 'yyyy-MM-dd

 ###DEPOSIT USER
 SELECT FROM_UNIXTIME(CAST(f_updated_at/1000 AS BIGINT),'yyyy-MM-dd') AS f_date
        ,COUNT(DISTINCT f_user_id) AS deposit_usercount
    FROM huobi_usa.ods_usa_bitex_t_deposit_virtual
    WHERE f_state IN (9,12,15) AND FROM_UNIXTIME(CAST(f_updated_at/1000 AS BIGINT), 'yyyy-MM-dd')='2018-08-21'
    GROUP BY FROM_UNIXTIME(CAST(f_updated_at/1000 AS BIGINT), 'yyyy-MM-dd')
 
 ###API DEAL USER
 SELECT FROM_UNIXTIME(CAST((f_updated_at/1000) AS BIGINT),'yyyy-MM-dd') AS f_date
        ,COUNT(DISTINCT f_user_id) AS api_usercount
    FROM huobi_usa.ods_usa_bitex_t_order 
    WHERE f_source IN (3,8) AND f_state IN (4,5,6) AND FROM_UNIXTIME(CAST((f_updated_at/1000) AS BIGINT),'yyyy-MM-dd') = '2018-08-21'
    GROUP BY FROM_UNIXTIME(CAST((f_updated_at/1000) AS BIGINT),'yyyy-MM-dd')
    
 ###DEAL USER
 SELECT f_date,COUNT(DISTINCT user_id) AS tex_usercount 
    FROM(
        SELECT FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd') AS f_date,f_user_id AS user_id
            FROM huobi_usa.ods_usa_bitex_t_match_result
            WHERE f_filled_amount > 0 AND f_role = 2
            GROUP BY FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd'),f_user_id  
        UNION ALL
        SELECT FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd') AS f_date,f_face_user_id AS user_id
            FROM huobi_usa.ods_usa_bitex_t_match_result
            WHERE f_filled_amount > 0 AND f_role = 2
            GROUP BY FROM_UNIXTIME(CAST((CAST(f_updated_at AS BIGINT)/1000) AS BIGINT),'yyyy-MM-dd'),f_face_user_id
        ) a
    WHERE f_date='2018-08-21'
    GROUP BY f_date

###WITHDRAW USER
SELECT FROM_UNIXTIME(CAST((f_updated_at/1000) AS BIGINT),'yyyy-MM-dd') AS f_date
        ,COUNT(DISTINCT f_user_id) AS withdraw_usercount
    FROM huobi_usa.ods_usa_bitex_t_withdraw_virtual
    WHERE FROM_UNIXTIME(CAST((f_updated_at/1000) AS BIGINT),'yyyy-MM-dd') ='2018-08-21' AND f_state = 11
    GROUP BY FROM_UNIXTIME(CAST((f_updated_at/1000) as bigint),'yyyy-MM-dd')
    
