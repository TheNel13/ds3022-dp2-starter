-- Type and clean the raw fragments.
-- distinct makes landing idempotent: a redelivered message (same order_no + word)
-- is counted once. received_at is left out on purpose so duplicates collapse.
select distinct
    cast(order_no as integer) as order_no,
    trim(word)                as word
from {{ source('raw', 'fragments') }}