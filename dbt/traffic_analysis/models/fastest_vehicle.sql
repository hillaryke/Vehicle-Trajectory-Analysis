WITH fastest_vehicle AS (
    SELECT
        MAX(max_speed) AS max_speed

    FROM {{ ref('vehicle_summary') }}
)

SELECT * FROM fastest_vehicle