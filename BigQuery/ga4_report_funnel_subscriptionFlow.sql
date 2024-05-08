WITH dataset AS (
    SELECT
        user_pseudo_id,
        event_name,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp
    FROM `meowtimes-416908.analytics_313673358.events_*`
    WHERE event_name IN ('click_pgProductDetail_Subscribe', 'click_pgSelectPeriod_nextStep', 'click_pgInputAddress_payment', 'view_pgProductOrderComplete')
),

click_pgProductDetail_Subscribe AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_pgProductDetail_Subscribe'
),

click_pgSelectPeriod_nextStep AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_pgSelectPeriod_nextStep'
),

click_pgInputAddress_payment AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_pgInputAddress_payment'
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
        cs.event_date,
        COUNT(DISTINCT cs.user_pseudo_id) AS click_pgProductDetail_Subscribe,
        COUNT(DISTINCT cn.user_pseudo_id) AS click_pgSelectPeriod_nextStep,
        COUNT(DISTINCT cp.user_pseudo_id) AS click_pgInputAddress_payment,
        COUNT(DISTINCT voc.user_pseudo_id) AS view_pgProductOrderComplete
    FROM click_pgProductDetail_Subscribe cs
    LEFT JOIN click_pgSelectPeriod_nextStep cn
        ON cs.user_pseudo_id = cn.user_pseudo_id
        AND cs.event_date = cn.event_date
        AND cs.event_timestamp < cn.event_timestamp
    LEFT JOIN click_pgInputAddress_payment cp
        ON cn.user_pseudo_id = cp.user_pseudo_id
        AND cn.event_date = cp.event_date
        AND cn.event_timestamp < cp.event_timestamp
    LEFT JOIN view_pgProductOrderComplete voc
        ON cp.user_pseudo_id = voc.user_pseudo_id
        AND cp.event_date = voc.event_date
        AND cp.event_timestamp < voc.event_timestamp
    GROUP BY 1
)

SELECT
    event_date,
    click_pgProductDetail_Subscribe,
    click_pgSelectPeriod_nextStep,
    click_pgInputAddress_payment,
    view_pgProductOrderComplete,
    1 AS subscription_start_rate,
    ROUND(COALESCE(click_pgSelectPeriod_nextStep / NULLIF(click_pgProductDetail_Subscribe,0), 0), 4) AS  select_period_rate,
    ROUND(COALESCE(click_pgInputAddress_payment / NULLIF(click_pgProductDetail_Subscribe,0), 0), 4) AS  input_address_rate,
    ROUND(COALESCE(view_pgProductOrderComplete / NULLIF(click_pgProductDetail_Subscribe,0), 0), 4) AS  pay_rate
FROM funnel
ORDER BY 1 DESC
