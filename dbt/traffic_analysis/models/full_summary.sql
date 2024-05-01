WITH full_summary AS (
    SELECT
        v.track_id,
        t.max_speed

    FROM {{ ref('vehicle_summary') }} v
    JOIN {{ ref('trajectory_summary') }} t ON v.track_id = t.track_id

)

SELECT * FROM full_summary