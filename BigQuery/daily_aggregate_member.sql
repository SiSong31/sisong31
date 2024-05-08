SELECT
    gm.mem_idx AS member_index,
    CASE
        WHEN gm.mem_gubun = '8' THEN '차단 회원'
        WHEN gm.mem_gubun = '9' THEN '탈퇴 회원'
        ELSE '일반 회원'
    END AS member_status,
    CASE
        WHEN gm.mem_id LIKE 'kko_%' THEN '카카오'
        WHEN gm.mem_id LIKE 'nid_%' THEN '네이버'
        WHEN gm.mem_id LIKE 'ggl_%' THEN '구글'
        ELSE '이메일'
    END AS join_method,
    gm.reg_date AS join_date,
    IF(ss.sche_stat IS NULL, 
        IF(sd.mem_idx IS NULL, '미구독', '구독 해지'), 
            IF(ss.sche_stat = 'SC10', '신청서 작성', 
                IF(ss.sche_stat = 'SC50', '구독 정지', 
                    IF(ss.sche_stat = 'SC90', '구독 해지', '구독중')
    ))) AS subscribe_status,
FROM(
    SELECT
        mem_idx,
        mem_gubun,
        mem_id,
        reg_date
    FROM bq_meowtimes_rdsmeowtimes.gi_member
    ) AS gm
LEFT JOIN(
    SELECT
        mem_idx,
        sche_stat,
        sche_term,
        next_order_date,
        last_order_date
    FROM bq_meowtimes_rdsmeowtimes.sche_subsc) AS ss 
ON gm.mem_idx = ss.mem_idx
LEFT JOIN (
    SELECT
    mem_idx 
    FROM bq_meowtimes_rdsmeowtimes.sche_del 
    GROUP BY mem_idx) AS sd
ON gm.mem_idx = sd.mem_idx
