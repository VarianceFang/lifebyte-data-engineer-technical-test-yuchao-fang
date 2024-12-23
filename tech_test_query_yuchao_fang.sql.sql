WITH active_trades AS (
    -- Get enabled users and records in June,July, August and September 2020
    SELECT DISTINCT
        t.login_hash,
        t.server_hash,
        -- Replace the unexpected symbols
        CASE WHEN t.symbol = 'USD,CHF' THEN 'USDCHF' ELSE t.symbol END,
        u.currency
    FROM trades t
    JOIN users u ON 
        t.login_hash = u.login_hash 
        AND t.server_hash = u.server_hash
    WHERE u.enable = 1
    AND t.close_time BETWEEN '2020-06-01' AND '2020-09-30'
), 
date_series AS (
    -- Generate all dates for 2020-06 to 2020-09
    SELECT generate_series(
        '2020-06-01'::date,
        '2020-09-30'::date,
        '1 day'::interval
    )::date as dt_report
),
base_combinations AS (
    -- Create combination for dt_report/login/server/symbol/currency every day in the time range
    SELECT 
        d.dt_report,
        at.login_hash,
        at.server_hash,
        at.symbol,
        at.currency
    FROM date_series d
    CROSS JOIN active_trades at
),
trade_metrics AS (
    -- Calculate all metrics
    SELECT 
        b.*,
        COALESCE(SUM(t.volume) FILTER (
            WHERE t.close_time <= b.dt_report 
            AND t.close_time >= b.dt_report - INTERVAL '6 days'
        ), 0) as sum_volume_prev_7d,
        COALESCE(SUM(t.volume) FILTER (
            WHERE t.close_time <= b.dt_report
        ), 0) as sum_volume_prev_all,
        COALESCE(SUM(t.volume) FILTER (
            WHERE DATE_TRUNC('month', t.close_time) = '2020-08-01'::date
            AND t.close_time <= b.dt_report
        ), 0) as sum_volume_2020_08,
        MIN(t.close_time) FILTER (
            WHERE t.close_time <= b.dt_report
        ) as date_first_trade,
        COUNT(t.ticket_hash) FILTER (
            WHERE t.close_time <= b.dt_report 
            AND t.close_time >= b.dt_report - INTERVAL '6 days'
        ) as trade_count_7d
    FROM base_combinations b
    LEFT JOIN trades t ON 
        b.login_hash = t.login_hash 
        AND b.server_hash = t.server_hash 
        AND b.symbol = t.symbol
        AND t.close_time <= b.dt_report
        AND t.close_time >= '2020-06-01'
        AND t.close_time <= '2020-09-30'
    GROUP BY 
        b.dt_report,
        b.login_hash,
        b.server_hash,
        b.symbol,
        b.currency
)
SELECT 
    dt_report,
    login_hash,
    server_hash,
    symbol,
    currency,
    sum_volume_prev_7d,
    sum_volume_prev_all,
    DENSE_RANK() OVER (
        PARTITION BY dt_report, symbol
        ORDER BY sum_volume_prev_7d DESC NULLS LAST
    ) as rank_volume_symbol_prev_7d,
    DENSE_RANK() OVER (
        PARTITION BY dt_report
        ORDER BY trade_count_7d DESC NULLS LAST
    ) as rank_count_prev_7d,
    sum_volume_2020_08,
    date_first_trade,
    ROW_NUMBER() OVER (
        ORDER BY dt_report DESC, login_hash, server_hash, symbol
    ) as row_number
FROM trade_metrics
ORDER BY row_number DESC;