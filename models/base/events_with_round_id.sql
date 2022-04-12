SELECT 
    e.*,
    r.id as round_id 
FROM {{ source('atomic','events') }} e
LEFT JOIN snowplow_atomic.com_askattest_round_1 r
    on e.event_id = r.root_id
    AND e.collector_tstamp = r.root_tstamp
    