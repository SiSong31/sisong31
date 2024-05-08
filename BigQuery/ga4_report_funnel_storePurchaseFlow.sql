WITH dataset AS (
    SELECT
        user_pseudo_id,
        event_name,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp
    FROM `meowtimes-416908.analytics_313673358.events_*`
    WHERE event_name IN ('view_pgsStore', 'click_pgStoreDetail_mdCart_goPay', 'click_pgStoreList_mdCart_goPay', 'click_pgStoreDetail_goBuy', 'click_pgStoreInputAddress_goPay', 'view_pgProductOrderComplete')
),

view_pgsStore AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'view_pgsStore'
),

go_buy AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name IN ('click_pgStoreDetail_mdCart_goPay', 'click_pgStoreList_mdCart_goPay', 'click_pgStoreDetail_goBuy')
),

click_pgStoreInputAddress_goPay AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_pgStoreInputAddress_goPay'
),

view_pgProductOrderComplete AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'view_pgProductOrderComplete'
),

funnel AS (
    SELECT
        vs.event_date,
        COUNT(DISTINCT vs.user_pseudo_id) AS view_pgsStore,
        COUNT(DISTINCT gb.user_pseudo_id) AS go_buy,
        COUNT(DISTINCT cgp.user_pseudo_id) AS click_pgStoreInputAddress_goPay,
        COUNT(DISTINCT voc.user_pseudo_id) AS view_pgProductOrderComplete
    FROM view_pgsStore vs
    LEFT JOIN go_buy gb
        ON vs.user_pseudo_id = gb.user_pseudo_id
        AND vs.event_date = gb.event_date
        AND vs.event_timestamp < gb.event_timestamp
    LEFT JOIN click_pgStoreInputAddress_goPay cgp
        ON gb.user_pseudo_id = cgp.user_pseudo_id
        AND gb.event_date = cgp.event_date
        AND gb.event_timestamp < cgp.event_timestamp
    LEFT JOIN view_pgProductOrderComplete voc
        ON cgp.user_pseudo_id = voc.user_pseudo_id
        AND cgp.event_date = voc.event_date
        AND cgp.event_timestamp < voc.event_timestamp
    GROUP BY 1
)

SELECT
    event_date,
    view_pgsStore,
    go_buy,
    click_pgStoreInputAddress_goPay,
    view_pgProductOrderComplete,
    1 AS view_store_page_rate,
    ROUND(COALESCE(go_buy / NULLIF(view_pgsStore,0), 0), 4) AS  go_buy_rate,
    ROUND(COALESCE(click_pgStoreInputAddress_goPay / NULLIF(view_pgsStore,0), 0), 4) AS  go_pay_rate,
    ROUND(COALESCE(view_pgProductOrderComplete / NULLIF(view_pgsStore,0), 0), 4) AS  order_complete_rate
FROM funnel
ORDER BY 1 DESC
