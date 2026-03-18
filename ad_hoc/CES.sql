WITH
	responses AS (
		SELECT
			id                                 AS response_id,
			hotjar_user_id,
			created_time,
			browser,
			country,
			device,
			os,
			response_origin_url,
			answers,
			answers.value:question_id::VARCHAR AS question_id,
			answers.value:answer::VARCHAR      AS answer
		FROM latest_vault.hotjar.survey_responses_se,
			 LATERAL FLATTEN(INPUT => latest_vault.hotjar.survey_responses_se.answers) AS answers
	),
	questions AS (
		SELECT
			id,
			questions,
			url,
			responses_url,
			question.value:id::VARCHAR   AS question_id,
			question.value:text::VARCHAR AS text
		FROM latest_vault.hotjar.surveys_se,
			 LATERAL FLATTEN(INPUT => latest_vault.hotjar.surveys_se.questions) AS question
	)
SELECT
	r.response_id,
	r.hotjar_user_id,
	r.created_time,
	r.browser,
	r.country,
	r.device,
	r.os,
	r.response_origin_url,
	se.data.PAGE_URL_CATEGORISATION(response_origin_url),
	r.question_id,
	q.url           AS source_url,
	q.responses_url AS response_url,
	q.text,
	r.answer
FROM responses r
INNER JOIN questions q
	ON q.question_id = r.question_id
-- WHERE hotjar_user_id = '048e8705-3f9e-5ed8-9792-5f0b45c6a2b6'
;

SELECT *
FROM latest_vault.hotjar.survey_responses_se
;

-- need to know if pre book or post book

SELECT *
FROM latest_vault.hotjar.surveys_se;