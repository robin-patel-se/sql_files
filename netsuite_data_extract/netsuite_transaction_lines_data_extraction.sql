SELECT *

FROM (

	SELECT
		t.transaction_id,
		tl.transaction_line_id AS transaction_lines_line_id,
		t.tranid,
		t.created_by_id,
		t.booking_id,
		t.cms_sale_id,
		t.concur_request_id,
		t.create_date,
		t.transaction_type,
		tl.memo                AS transaction_lines_memo,
		a.accountnumber        AS accounts_account_number,
		a.name                 AS accounts_name,
		a.type_name            AS accounts_type_name,
		ac.name                AS accounting_periods_name,
		c.name                 AS currency_name,
		e.full_name            AS entity_full_name,
		e.name                 AS entity_name,
		s.full_name            AS subsidiaries_full_name,
		s.name                 AS subsidiaries_name,
		tl.amount              AS transaction_lines_amount

	FROM transaction_lines tl
		-- JOINS TO TRANSACTION_LINES TABLE
		JOIN      accounts a
				  ON a.account_id = tl.account_id
		LEFT JOIN subsidiaries s
				  ON s.subsidiary_id = tl.subsidiary_id
					  -- JOINS TO TRANSACTIONS TABLE
		JOIN      transactions t
				  ON tl.transaction_id = t.transaction_id
		JOIN      accounting_periods ac
				  ON ac.accounting_period_id = t.accounting_period_id
		JOIN      currencies c
				  ON c.currency_id = t.currency_id
		LEFT JOIN entity e
				  ON e.entity_id = t.entity_id
)
WHERE accounting_periods_name = 'Dec 2023' -- simply adjust the period month at a time and export
;

------------------------------------------------------------------------------------------------------------------------
