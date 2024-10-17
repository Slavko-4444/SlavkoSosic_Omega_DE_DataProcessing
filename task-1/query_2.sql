with modified_table as (select *, case
when is_successful='yes'
and lag(is_successful,2) over (partition by user_id, transaction_category order by created_at)='yes'
and lag(is_successful) over (partition by user_id, transaction_category order by created_at)='yes'
and LEAD(is_successful) over (partition by user_id, transaction_category order by created_at)='yes'
then 1 else 0
end as consec
from transactions
)

select distinct(user_id) from modified_table
where consec=1;
