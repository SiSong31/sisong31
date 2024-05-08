SELECT
    DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY) AS record_date,
    SUM(CASE WHEN order_type = 1 AND odr_gubun <> 'NOR' THEN pay_amt - delv_price ELSE 0 END) AS subsc_product_revenue,
    SUM(CASE WHEN order_type = 1 AND odr_gubun <> 'NOR' THEN delv_price ELSE 0 END) AS subsc_delivery_revenue,
    SUM(CASE WHEN order_type = 1 AND odr_gubun <> 'NOR' THEN pay_amt ELSE 0 END) AS subsc_revenue,
    SUM(CASE WHEN order_type = 1 AND odr_gubun <> 'NOR' THEN 1 ELSE 0 END) AS subsc_order_cnt,
    SUM(CASE WHEN order_type = 1 AND odr_gubun = 'NOR' THEN pay_amt - delv_price ELSE 0 END) AS normal_product_revenue,
    SUM(CASE WHEN order_type = 1 AND odr_gubun = 'NOR' THEN delv_price ELSE 0 END) AS normal_delivery_revenue,
    SUM(CASE WHEN order_type = 1 AND odr_gubun = 'NOR' THEN pay_amt ELSE 0 END) AS normal_revenue,
    SUM(CASE WHEN order_type = 1 AND odr_gubun = 'NOR' THEN 1 ELSE 0 END) AS normal_order_cnt,
    SUM(CASE WHEN order_type = 1 THEN pay_amt ELSE 0 END) AS subsc_normal_revenue,
    SUM(CASE WHEN order_type = 2 THEN pay_amt - delv_price ELSE 0 END) AS store_product_revenue,
    SUM(CASE WHEN order_type = 2 THEN delv_price ELSE 0 END) AS store_delivery_revenue,
    SUM(CASE WHEN order_type = 2 THEN pay_amt ELSE 0 END) AS store_revenue,
    SUM(CASE WHEN order_type = 2 THEN 1 ELSE 0 END) AS store_order_cnt,
    SUM(pay_amt) AS total_revenue,
    (
        SELECT SUM(POINT)
        FROM bq_meowtimes_rdsmeowtimes.POINT_INFO
        WHERE TYPE IN ('PT0101', 'PT0102', 'PT0103', 'PT0104', 'PT0107', 'PT0111', 'PT0121', 'PT0202', 'PT0203', 'PT0204')
          AND DATE(CREATE_TIME) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    ) AS points_earned,
    (
        SELECT SUM(POINT)
        FROM bq_meowtimes_rdsmeowtimes.POINT_INFO
        WHERE TYPE IN ('PT0105', 'PT0201')
          AND DATE(CREATE_TIME) = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    ) AS points_spent,
    SUM(CASE WHEN order_type = 1 THEN goods_qty ELSE 0 END) AS sold_qty_product,
    SUM(CASE WHEN order_type = 2 THEN goods_qty ELSE 0 END) AS sold_qty_store,
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
