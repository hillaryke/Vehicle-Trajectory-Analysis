-- You can pull data from source using the source function and picks data from orders table
select
    *
from {{ source('tpch', 'orders') }}