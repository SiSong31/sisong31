SELECT
    DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY) AS record_date,
    SUM(GM.new_join) AS new_member_cnt,
    SUM(SS.new_member) AS new_subscriber_cnt,
    SUM(CASE WHEN SS.sche_stat = 'SC20' THEN SH.resume_member ELSE 0 END) AS resumed_subscriber_cnt,
    SUM(SS.stop_member) AS stopped_subscriber_cnt,
    SUM(CASE WHEN SS.sche_stat IS NULL THEN SD.close_member ELSE 0 END) AS unsubscriber_cnt
FROM (
    SELECT
        mem_idx,
        IF(DATE(reg_date) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), 1, 0) AS new_join
    FROM 
        bq_meowtimes_rdsmeowtimes.gi_member
    WHERE
        mem_gubun = '0' -- 탈퇴 회원 제외
) AS GM
LEFT JOIN (
    SELECT
        mem_idx,
        sche_stat,
        IF(sche_stat = 'SC20' AND DATE(start_date) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), 1, 0) AS new_member,
        IF(sche_stat = 'SC50' AND DATE(cancel_date) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), 1, 0) AS stop_member
    FROM bq_meowtimes_rdsmeowtimes.sche_subsc
) AS SS
ON GM.mem_idx = SS.mem_idx
LEFT JOIN (
    SELECT
        INDEX_MEMBER,
        1 AS resume_member
    FROM bq_meowtimes_rdsmeowtimes.SCHEDULE_HISTORY
    WHERE
        SCHEDULE_BEFORE = "SC50"
        AND SCHEDULE_AFTER = "SC20"
        AND DATE(MODIFY_DATE) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    GROUP BY INDEX_MEMBER
) AS SH
ON GM.mem_idx = SH.INDEX_MEMBER
LEFT JOIN (
    SELECT
        mem_idx,
        1 AS close_member
    FROM bq_meowtimes_rdsmeowtimes.sche_del
    WHERE DATE(del_dtm) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    GROUP BY mem_idx
) AS SD
ON GM.mem_idx = SD.mem_idx
