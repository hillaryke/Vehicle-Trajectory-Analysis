WITH trajectory_summary AS (
    SELECT
        track_id,
        MAX(speed) AS max_speed

    FROM trajectories
    GROUP BY track_id
)

SELECT * FROM trajectory_summary