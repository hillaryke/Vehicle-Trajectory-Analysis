WITH total_vehicles_per_type AS (
    SELECT
        COUNT(DISTINCT v.type) AS total_vehicles,
        SUM(vsummary.total_distance) AS total_distance

    FROM {{ ref('vehicle_summary')}} vsummary
    JOIN vehicles v ON v.track_id = vsummary.track_id
    GROUP BY v.type
)

SELECT * FROM total_vehicles_per_type