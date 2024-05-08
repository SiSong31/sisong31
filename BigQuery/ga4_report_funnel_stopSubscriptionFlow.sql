WITH dataset AS (
    SELECT
        user_pseudo_id,
        event_name,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp
    FROM `meowtimes-416908.analytics_313673358.events_*`
    WHERE event_name IN ('click_startStopSubscription', 'click_startStopSubscriptionForm', 'click_stopSubscriptionComplete')
),

click_startStopSubscription AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_startStopSubscription'
),

click_startStopSubscriptionForm AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_startStopSubscriptionForm'
),

click_stopSubscriptionComplete AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_stopSubscriptionComplete'
),

funnel AS (
    SELECT
        ss.event_date,
        COUNT(DISTINCT ss.user_pseudo_id) AS click_startStopSubscription,
        COUNT(DISTINCT fs.user_pseudo_id) AS click_startStopSubscriptionForm,
        COUNT(DISTINCT fc.user_pseudo_id) AS click_stopSubscriptionComplete
    FROM click_startStopSubscription ss
    LEFT JOIN click_startStopSubscriptionForm fs
        ON ss.user_pseudo_id = fs.user_pseudo_id
        AND ss.event_date = fs.event_date
        AND ss.event_timestamp < fs.event_timestamp
    LEFT JOIN click_stopSubscriptionComplete fc
        ON fs.user_pseudo_id = fc.user_pseudo_id
        AND fs.event_date = fc.event_date
        AND fs.event_timestamp < fc.event_timestamp
    GROUP BY 1
)

SELECT
    event_date,
    click_startStopSubscription,
    click_startStopSubscriptionForm,
    click_stopSubscriptionComplete,
    1 AS start_subscription_rate,
    ROUND(COALESCE(click_startStopSubscriptionForm / NULLIF(click_startStopSubscription,0), 0), 4) AS  form_start_rate,
    ROUND(COALESCE(click_stopSubscriptionComplete / NULLIF(click_startStopSubscription,0), 0), 4) AS  form_complete_rate
FROM funnel
ORDER BY 1 DESC
