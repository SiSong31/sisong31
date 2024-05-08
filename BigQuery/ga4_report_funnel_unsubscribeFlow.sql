WITH dataset AS (
    SELECT
        user_pseudo_id,
        event_name,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp
    FROM `meowtimes-416908.analytics_313673358.events_*`
    WHERE event_name IN ('click_startUnsubscribe', 'click_startUnsubscriptionForm', 'click_UnsubscriptionFormComplete')
),
  
click_startUnsubscribe AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_startUnsubscribe'
),
  
click_startUnsubscriptionForm AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_startUnsubscriptionForm'
),
  
click_UnsubscriptionFormComplete AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_UnsubscriptionFormComplete'
),

funnel AS (
    SELECT
        su.event_date,
        COUNT(DISTINCT su.user_pseudo_id) AS click_startUnsubscribe,
        COUNT(DISTINCT sf.user_pseudo_id) AS click_startUnsubscriptionForm,
        COUNT(DISTINCT fc.user_pseudo_id) AS click_UnsubscriptionFormComplete
    FROM click_startUnsubscribe su
    LEFT JOIN click_startUnsubscriptionForm sf
        ON su.user_pseudo_id = sf.user_pseudo_id
        AND su.event_date = sf.event_date
        AND su.event_timestamp < sf.event_timestamp
    LEFT JOIN click_UnsubscriptionFormComplete fc
        ON sf.user_pseudo_id = fc.user_pseudo_id
        AND sf.event_date = fc.event_date
        AND sf.event_timestamp < fc.event_timestamp
    GROUP BY 1
)

SELECT
    event_date,
    click_startUnsubscribe,
    click_startUnsubscriptionForm,
    click_UnsubscriptionFormComplete,
    1 AS start_unsubscribe_rate,
    ROUND(COALESCE(click_startUnsubscriptionForm / NULLIF(click_startUnsubscribe,0), 0), 4) AS  form_start_rate,
    ROUND(COALESCE(click_UnsubscriptionFormComplete / NULLIF(click_startUnsubscribe,0), 0), 4) AS  form_complete_rate
FROM funnel
ORDER BY 1 DESC
