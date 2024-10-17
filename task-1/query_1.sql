
WITH ConsecutiveTransactions as (
SELECT 
	user_id, 
	transaction_category,
	created_at,
	is_successful,
	LAG(is_successful) OVER (PARTITION BY user_id, transaction_category ORDER BY created_at) AS prev_success_2,
	LAG(is_successful, 2) OVER (PARTITION BY user_id, transaction_category ORDER BY created_at) AS prev_success_3,
	LAG(is_successful, 3) OVER (PARTITION BY user_id, transaction_category ORDER BY created_at) AS prev_success_4
FROM transactions
WHERE transaction_category = 'credit_card' 
)
SELECT DISTINCT user_id, transaction_category from ConsecutiveTransactions
WHERE is_successful = 'yes' and
prev_success_2 ='yes' and
prev_success_3 ='yes' and
prev_success_4 ='yes';
