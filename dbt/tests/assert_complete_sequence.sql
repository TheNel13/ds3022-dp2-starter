-- Singular test: returns a row (= FAIL) unless there are exactly
-- expected_fragments fragments numbered 1..N with no gaps.
with s as (
    select
        count(*)                 as n_rows,
        count(distinct order_no) as n_distinct,
        min(order_no)            as lo,
        max(order_no)            as hi
    from {{ ref('stg_fragments') }}
)
select *
from s
where n_rows     != {{ var('expected_fragments') }}
   or n_distinct != {{ var('expected_fragments') }}
   or lo         != 1
   or hi         != {{ var('expected_fragments') }}