WITH dataset AS (
    SELECT
        user_pseudo_id,
        event_name,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp
    FROM `meowtimes-416908.analytics_313673358.events_*`
    WHERE event_name IN ('click_pgLogIn_join', 'formStart_pgjoin', 'complete_registration')
),
click_pgLogIn_join AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'click_pgLogIn_join'
),
formStart_pgjoin AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'formStart_pgjoin'
),
complete_registration AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_timestamp
    FROM dataset
    WHERE event_name = 'complete_registration'
),
funnel AS (
    SELECT
        cj.event_date,
        COUNT(DISTINCT cj.user_pseudo_id) AS click_pgLogIn_join,
        COUNT(DISTINCT sfs.user_pseudo_id) AS formStart_pgjoin,
        COUNT(DISTINCT rc.user_pseudo_id) AS complete_registration
    FROM click_pgLogIn_join cj
    LEFT JOIN formStart_pgjoin sfs
        ON cj.user_pseudo_id = sfs.user_pseudo_id
        AND cj.event_date = sfs.event_date
        AND cj.event_timestamp < sfs.event_timestamp
    LEFT JOIN complete_registration rc
        ON sfs.user_pseudo_id = rc.user_pseudo_id
        AND sfs.event_date = rc.event_date
        AND sfs.event_timestamp < rc.event_timestamp
    GROUP BY 1
)
SELECT
    event_date,
    click_pgLogIn_join,
    formStart_pgjoin,
    complete_registration,
    1 AS start_signUp_rate,
    ROUND(COALESCE(formStart_pgjoin / NULLIF(click_pgLogIn_join,0), 0), 4) AS  signUp_form_start_rate,
    ROUND(COALESCE(complete_registration / NULLIF(click_pgLogIn_join,0), 0), 4) AS  form_submit_rate
FROM funnel
ORDER BY 1 DESC
