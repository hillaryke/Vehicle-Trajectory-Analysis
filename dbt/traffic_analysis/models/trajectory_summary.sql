WITH trajectory_summary AS (
    SELECT
        track_id,
        MAX(speed) AS max_speed,
        COUNT(time) AS num_points

    FROM trajectories
    GROUP BY track_id
)

SELECT * FROM trajectory_summary