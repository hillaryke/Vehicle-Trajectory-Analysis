WITH types_summary AS (
    SELECT
        type,
        COUNT(*) AS count

    FROM vehicles
    GROUP BY type
)

SELECT * FROM vehicles