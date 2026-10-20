-- One row: the words joined in integer order_no order.
-- ref() makes dbt build staging first and SKIP this model if a test on staging fails.
select
    string_agg(word, ' ' order by order_no) as phrase,
    count(*)                                as n_fragments
from {{ ref('stg_fragments') }}