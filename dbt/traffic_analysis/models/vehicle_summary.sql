WITH vehicle_summary AS (
    SELECT
        v.track_id,
        MAX(t.speed) AS max_speed,
        MIN(t.speed) AS min_speed,
        MIN(t.time) AS fastest_time,
        SUM(t.time) AS total_time,
        SUM(v.traveled_d) AS total_distance

    FROM vehicles v
    JOIN trajectories t ON v.track_id = t.track_id
    GROUP BY v.track_id
)

SELECT * FROM vehicle_summary