with t as (
SELECT
  acs.account_id as account_id,
  count(es.id_message) over ( PARTITION BY acs.account_id, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) as sent_msg,
  DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH) AS sent_month,
  MIN(s.date) OVER (PARTITION BY acs.account_id, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) as first_sent_date,
  Max(s.date) OVER (PARTITION BY acs.account_id, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) as last_sent_date
 FROM
    `DA.email_sent`es
    join DA.account_session acs
    on es.id_account = acs.account_id
  Join `DA.session`s
  on acs.ga_session_id = s.ga_session_id
)


select
  distinct t.sent_month,
  t.account_id as id_account,
  t.sent_msg / sum(t.sent_msg) over ( PARTITION BY t.sent_month)*100 as sent_msg_percent_from_this_month,
  t.first_sent_date,
  t.last_sent_date
FROM t
   order by  sent_month desc
