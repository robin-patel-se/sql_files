SELECT *
FROM (
	SELECT
		transaction_id    AS system_notes_transaction_id,
		date_created      AS system_notes_date_created,
		ap.name           AS accounting_periods_name,
		context_type_name AS system_notes_context_type_name
	FROM system_notes
		JOIN transactions t ON system_notes.transaction_id = t.transaction_id
		JOIN accounting_periods ap ON ap.accounting_period_id = t.accounting_period_id
)
WHERE accounting_periods_name = 'Dec 2023' -- simply adjust the period month at a time and export