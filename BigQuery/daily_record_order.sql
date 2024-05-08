SELECT
    DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY) AS record_date,
    order_type,
    order_no,
    OD.mem_idx,
    odr_gubun,
    pay_amt,
    delv_price,
    goods_qty,
    (CASE
      WHEN order_type = 1 AND odr_gubun <> 'NOR' THEN '구독 주문'
      WHEN order_type = 1 AND odr_gubun = 'NOR' THEN '일반 주문'
      WHEN order_type = 2 THEN '스토어 주문'
      ELSE '분류 불가' END
      ) AS order_sort,
FROM(
    SELECT
        1 AS order_type,
        GDSO.order_no,
        GDSO.mem_idx,
        GDSO.odr_gubun,
        GDSO.pay_amt,
        GDSO.delv_price,
        GOI.goods_qty
    FROM
        bq_meowtimes_rdsmeowtimes.goods_order AS GDSO
    INNER JOIN(
        SELECT
            order_no,
            SUM(goods_qty) AS goods_qty
        FROM
            bq_meowtimes_rdsmeowtimes.goods_order_items
        GROUP BY
            order_no
    ) AS GOI
    ON GDSO.order_no = GOI.order_no
    WHERE
        GDSO.odr_stat IN ('OD20', 'OD30', 'OD40', 'OD50', 'OD60', 'OD70', 'OD75', 'OD80')
        AND DATE(GDSO.pay_dtm) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    UNION ALL
    SELECT
        2 AS order_type,
        SO.order_no,
        SO.mem_idx,
        '' AS odr_gubun,
        SO.pay_amt,
        SO.delv_price,
        SOI.goods_qty
    FROM bq_meowtimes_rdsmeowtimes.shop_order AS SO
    INNER JOIN (
        SELECT
            order_no,
            SUM(odr_qty) AS goods_qty
        FROM
            bq_meowtimes_rdsmeowtimes.shop_order_items
        GROUP BY
            order_no
    ) AS SOI
        ON SO.order_no = SOI.order_no
    WHERE
        odr_stat IN ('OD20', 'OD30', 'OD40', 'OD50', 'OD60', 'OD70', 'OD75', 'OD80')
        AND DATE(pay_dtm) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
) AS OD
INNER JOIN
    bq_meowtimes_rdsmeowtimes.gi_member AS GM
ON OD.mem_idx = GM.mem_idx
