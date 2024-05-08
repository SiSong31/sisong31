WITH dataset AS (
    SELECT
        user_pseudo_id,
        event_name,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp
    FROM
        `meowtimes-416908.analytics_313673358.events_*`
    WHERE
        event_name IN ('click_allPages_header_viewCart', 'click_listPages_bagIcon_viewCart', 'click_subscriptionDetail_bagIcon_viewCar', 'click_subscriptionDetail_normalOrder_vie', 'click_storeDetail_cart_viewCart', 'click_allPages_mdCart_cartMinus', 'click_allPages_mdCart_cartPlus', 'click_allPages_mdCart_cartDelete')
),

a1 AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name = 'click_allPages_header_viewCart'
),

a2 AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name = 'click_listPages_bagIcon_viewCart'
),

a3 AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name = 'click_subscriptionDetail_bagIcon_viewCar'
),

a4 AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name = 'click_subscriptionDetail_normalOrder_vie'
),

a5 AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name = 'click_storeDetail_cart_viewCart'
),

min AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name = 'click_allPages_mdCart_cartMinus'
),

plus AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name = 'click_allPages_mdCart_cartPlus'
),

del AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name = 'click_allPages_mdCart_cartDelete'
),

open_cart AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name IN ('click_allPages_header_viewCart', 'click_listPages_bagIcon_viewCart', 'click_subscriptionDetail_bagIcon_viewCar', 'click_subscriptionDetail_normalOrder_vie', 'click_storeDetail_cart_viewCart')
),

edit_cart AS(
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM
        dataset
    WHERE
        event_name IN ('click_allPages_mdCart_cartMinus', 'click_allPages_mdCart_cartPlus', 'click_allPages_mdCart_cartDelete')
),

funnel1 AS (
    SELECT
        a1.event_date as event_date,
        COUNT(DISTINCT a1.user_pseudo_id) as funnel1_a1,
        COUNT(DISTINCT min.user_pseudo_id) as funnel1_a1_min,
        COUNT(DISTINCT plus.user_pseudo_id) as funnel1_a1_plus,
        COUNT(DISTINCT del.user_pseudo_id) as funnel1_a1_del,
        COUNT(DISTINCT edit_cart.user_pseudo_id) as funnel1_a1_editcart,
    FROM a1
    LEFT JOIN min
        ON a1.user_pseudo_id = min.user_pseudo_id
        AND a1.event_date = min.event_date
        AND a1.event_timestamp < min.event_timestamp
    LEFT JOIN plus
        ON a1.user_pseudo_id = plus.user_pseudo_id
        AND a1.event_date = plus.event_date
        AND a1.event_timestamp < plus.event_timestamp
    LEFT JOIN del
        ON a1.user_pseudo_id = del.user_pseudo_id
        AND a1.event_date = del.event_date
        AND a1.event_timestamp < del.event_timestamp
    LEFT JOIN edit_cart
        ON a1.user_pseudo_id = edit_cart.user_pseudo_id
        AND a1.event_date = edit_cart.event_date
        AND a1.event_timestamp < edit_cart.event_timestamp
    GROUP BY 1
),

funnel2 AS (
    SELECT
        a2.event_date as event_date,
        COUNT(DISTINCT a2.user_pseudo_id) as funnel2_a2,
        COUNT(DISTINCT min.user_pseudo_id) as funnel2_a2_min,
        COUNT(DISTINCT plus.user_pseudo_id) as funnel2_a2_plus,
        COUNT(DISTINCT del.user_pseudo_id) as funnel2_a2_del,
        COUNT(DISTINCT edit_cart.user_pseudo_id) as funnel2_a2_editcart,
    FROM a2
    LEFT JOIN min
        ON a2.user_pseudo_id = min.user_pseudo_id
        AND a2.event_date = min.event_date
        AND a2.event_timestamp < min.event_timestamp
    LEFT JOIN plus
        ON a2.user_pseudo_id = plus.user_pseudo_id
        AND a2.event_date = plus.event_date
        AND a2.event_timestamp < plus.event_timestamp
    LEFT JOIN del
        ON a2.user_pseudo_id = del.user_pseudo_id
        AND a2.event_date = del.event_date
        AND a2.event_timestamp < del.event_timestamp
    LEFT JOIN edit_cart
        ON a2.user_pseudo_id = edit_cart.user_pseudo_id
        AND a2.event_date = edit_cart.event_date
        AND a2.event_timestamp < edit_cart.event_timestamp
    GROUP BY 1
),

funnel3 AS (
    SELECT
        a3.event_date as event_date,
        COUNT(DISTINCT a3.user_pseudo_id) as funnel3_a3,
        COUNT(DISTINCT min.user_pseudo_id) as funnel3_a3_min,
        COUNT(DISTINCT plus.user_pseudo_id) as funnel3_a3_plus,
        COUNT(DISTINCT del.user_pseudo_id) as funnel3_a3_del,
        COUNT(DISTINCT edit_cart.user_pseudo_id) as funnel3_a3_editcart,
    FROM a3
    LEFT JOIN min
        ON a3.user_pseudo_id = min.user_pseudo_id
        AND a3.event_date = min.event_date
        AND a3.event_timestamp < min.event_timestamp
    LEFT JOIN plus
        ON a3.user_pseudo_id = plus.user_pseudo_id
        AND a3.event_date = plus.event_date
        AND a3.event_timestamp < plus.event_timestamp
    LEFT JOIN del
        ON a3.user_pseudo_id = del.user_pseudo_id
        AND a3.event_date = del.event_date
        AND a3.event_timestamp < del.event_timestamp
    LEFT JOIN edit_cart
        ON a3.user_pseudo_id = edit_cart.user_pseudo_id
        AND a3.event_date = edit_cart.event_date
        AND a3.event_timestamp < edit_cart.event_timestamp
    GROUP BY 1
),

funnel4 AS (
    SELECT
        a4.event_date as event_date,
        COUNT(DISTINCT a4.user_pseudo_id) as funnel4_a4,
        COUNT(DISTINCT min.user_pseudo_id) as funnel4_a4_min,
        COUNT(DISTINCT plus.user_pseudo_id) as funnel4_a4_plus,
        COUNT(DISTINCT del.user_pseudo_id) as funnel4_a4_del,
        COUNT(DISTINCT edit_cart.user_pseudo_id) as funnel4_a4_editcart,
    FROM a4
    LEFT JOIN min
        ON a4.user_pseudo_id = min.user_pseudo_id
        AND a4.event_date = min.event_date
        AND a4.event_timestamp < min.event_timestamp
    LEFT JOIN plus
        ON a4.user_pseudo_id = plus.user_pseudo_id
        AND a4.event_date = plus.event_date
        AND a4.event_timestamp < plus.event_timestamp
    LEFT JOIN del
        ON a4.user_pseudo_id = del.user_pseudo_id
        AND a4.event_date = del.event_date
        AND a4.event_timestamp < del.event_timestamp
    LEFT JOIN edit_cart
        ON a4.user_pseudo_id = edit_cart.user_pseudo_id
        AND a4.event_date = edit_cart.event_date
        AND a4.event_timestamp < edit_cart.event_timestamp
    GROUP BY 1
),

funnel5 AS (
    SELECT
        a5.event_date as event_date,
        COUNT(DISTINCT a5.user_pseudo_id) as funnel5_a5,
        COUNT(DISTINCT min.user_pseudo_id) as funnel5_a5_min,
        COUNT(DISTINCT plus.user_pseudo_id) as funnel5_a5_plus,
        COUNT(DISTINCT del.user_pseudo_id) as funnel5_a5_del,
        COUNT(DISTINCT edit_cart.user_pseudo_id) as funnel5_a5_editcart,
    FROM a5
    LEFT JOIN min
        ON a5.user_pseudo_id = min.user_pseudo_id
        AND a5.event_date = min.event_date
        AND a5.event_timestamp < min.event_timestamp
    LEFT JOIN plus
        ON a5.user_pseudo_id = plus.user_pseudo_id
        AND a5.event_date = plus.event_date
        AND a5.event_timestamp < plus.event_timestamp
    LEFT JOIN del
        ON a5.user_pseudo_id = del.user_pseudo_id
        AND a5.event_date = del.event_date
        AND a5.event_timestamp < del.event_timestamp
    LEFT JOIN edit_cart
        ON a5.user_pseudo_id = edit_cart.user_pseudo_id
        AND a5.event_date = edit_cart.event_date
        AND a5.event_timestamp < edit_cart.event_timestamp
    GROUP BY 1
),

funnel6 AS (
    SELECT
        open_cart.event_date as event_date,
        COUNT(DISTINCT open_cart.user_pseudo_id) as funnel6_opencart,
        COUNT(DISTINCT min.user_pseudo_id) as funnel6_opencart_min,
        COUNT(DISTINCT plus.user_pseudo_id) as funnel6_opencart_plus,
        COUNT(DISTINCT del.user_pseudo_id) as funnel6_opencart_del,
        COUNT(DISTINCT edit_cart.user_pseudo_id) as funnel6_opencart_editcart,
    FROM open_cart
    LEFT JOIN min
        ON open_cart.user_pseudo_id = min.user_pseudo_id
        AND open_cart.event_date = min.event_date
        AND open_cart.event_timestamp < min.event_timestamp
    LEFT JOIN plus
        ON open_cart.user_pseudo_id = plus.user_pseudo_id
        AND open_cart.event_date = plus.event_date
        AND open_cart.event_timestamp < plus.event_timestamp
    LEFT JOIN del
        ON open_cart.user_pseudo_id = del.user_pseudo_id
        AND open_cart.event_date = del.event_date
        AND open_cart.event_timestamp < del.event_timestamp
    LEFT JOIN edit_cart
        ON open_cart.user_pseudo_id = edit_cart.user_pseudo_id
        AND open_cart.event_date = edit_cart.event_date
        AND open_cart.event_timestamp < edit_cart.event_timestamp
    GROUP BY 1
),

cnt_a1 AS(
    SELECT
        a1.event_date as event_date,
        COUNT(DISTINCT a1.user_pseudo_id) as unq_user_cnt_a1,
        COUNT(a1.user_pseudo_id) as act_cnt_a1
    FROM a1
    GROUP BY 1
),

cnt_a2 AS(
    SELECT
        a2.event_date as event_date,
        COUNT(DISTINCT a2.user_pseudo_id) as unq_user_cnt_a2,
        COUNT(a2.user_pseudo_id) as act_cnt_a2
    FROM a2
    GROUP BY 1
),

cnt_a3 AS(
    SELECT
        a3.event_date as event_date,
        COUNT(DISTINCT a3.user_pseudo_id) as unq_user_cnt_a3,
        COUNT(a3.user_pseudo_id) as act_cnt_a3
    FROM a3
    GROUP BY 1
),

cnt_a4 AS(
    SELECT
        a4.event_date as event_date,
        COUNT(DISTINCT a4.user_pseudo_id) as unq_user_cnt_a4,
        COUNT(a4.user_pseudo_id) as act_cnt_a4
    FROM a4
    GROUP BY 1
),

cnt_a5 AS(
    SELECT
        a5.event_date as event_date,
        COUNT(DISTINCT a5.user_pseudo_id) as unq_user_cnt_a5,
        COUNT(a5.user_pseudo_id) as act_cnt_a5
    FROM a5
    GROUP BY 1
),

cnt_open_cart AS(
    SELECT
        open_cart.event_date as event_date,
        COUNT(DISTINCT open_cart.user_pseudo_id) as unq_user_cnt_open_cart,
        COUNT(open_cart.user_pseudo_id) as act_cnt_open_cart
    FROM open_cart
    GROUP BY 1
),

cnt_min AS(
    SELECT
        min.event_date as event_date,
        COUNT(DISTINCT min.user_pseudo_id) as unq_user_cnt_min,
        COUNT(min.user_pseudo_id) as act_cnt_min
    FROM min
    GROUP BY 1
),

cnt_plus AS(
    SELECT
        plus.event_date as event_date,
        COUNT(DISTINCT plus.user_pseudo_id) as unq_user_cnt_plus,
        COUNT(plus.user_pseudo_id) as act_cnt_plus
    FROM plus
    GROUP BY 1
),

cnt_del AS(
    SELECT
        del.event_date as event_date,
        COUNT(DISTINCT del.user_pseudo_id) as unq_user_cnt_del,
        COUNT(del.user_pseudo_id) as act_cnt_del
    FROM del
    GROUP BY 1
),

cnt_edit_cart AS(
    SELECT
        edit_cart.event_date as event_date,
        COUNT(DISTINCT edit_cart.user_pseudo_id) as unq_user_cnt_edit_cart,
        COUNT(edit_cart.user_pseudo_id) as act_cnt_edit_cart
    FROM edit_cart
    GROUP BY 1
)

SELECT
    cnt_open_cart.event_date as event_date,
    unq_user_cnt_a1,
    unq_user_cnt_a2,
    unq_user_cnt_a3,
    unq_user_cnt_a4,
    unq_user_cnt_a5,
    unq_user_cnt_min,
    unq_user_cnt_plus,
    unq_user_cnt_del,
    unq_user_cnt_open_cart,
    unq_user_cnt_edit_cart,
    act_cnt_a1,
    act_cnt_a2,
    act_cnt_a3,
    act_cnt_a4,
    act_cnt_a5,
    act_cnt_min,
    act_cnt_plus,
    act_cnt_del,
    act_cnt_open_cart,
    act_cnt_edit_cart,
    funnel1_a1,
    funnel1_a1_min,
    funnel1_a1_plus,
    funnel1_a1_del,
    funnel1_a1_editcart,
    funnel2_a2,
    funnel2_a2_min,
    funnel2_a2_plus,
    funnel2_a2_del,
    funnel2_a2_editcart,
    funnel3_a3,
    funnel3_a3_min,
    funnel3_a3_plus,
    funnel3_a3_del,
    funnel3_a3_editcart,
    funnel4_a4,
    funnel4_a4_min,
    funnel4_a4_plus,
    funnel4_a4_del,
    funnel4_a4_editcart,
    funnel5_a5,
    funnel5_a5_min,
    funnel5_a5_plus,
    funnel5_a5_del,
    funnel5_a5_editcart,
    funnel6_opencart,
    funnel6_opencart_min,
    funnel6_opencart_plus,
    funnel6_opencart_del,
    funnel6_opencart_editcart

FROM cnt_open_cart
LEFT JOIN cnt_a1
    ON cnt_open_cart.event_date = cnt_a1.event_date
LEFT JOIN cnt_a2
    ON cnt_open_cart.event_date = cnt_a2.event_date
LEFT JOIN cnt_a3
    ON cnt_open_cart.event_date = cnt_a3.event_date    
LEFT JOIN cnt_a4
    ON cnt_open_cart.event_date = cnt_a4.event_date
LEFT JOIN cnt_a5
    ON cnt_open_cart.event_date = cnt_a5.event_date
LEFT JOIN cnt_min
    ON cnt_open_cart.event_date = cnt_min.event_date
LEFT JOIN cnt_plus
    ON cnt_open_cart.event_date = cnt_plus.event_date
LEFT JOIN cnt_del
    ON cnt_open_cart.event_date = cnt_del.event_date    
LEFT JOIN cnt_edit_cart
    ON cnt_open_cart.event_date = cnt_edit_cart.event_date
LEFT JOIN funnel1
    ON cnt_open_cart.event_date = funnel1.event_date
LEFT JOIN funnel2
    ON cnt_open_cart.event_date = funnel2.event_date
LEFT JOIN funnel3
    ON cnt_open_cart.event_date = funnel3.event_date
LEFT JOIN funnel4
    ON cnt_open_cart.event_date = funnel4.event_date
LEFT JOIN funnel5
    ON cnt_open_cart.event_date = funnel5.event_date
LEFT JOIN funnel6
    ON cnt_open_cart.event_date = funnel6.event_date

ORDER BY
    event_date DESC
