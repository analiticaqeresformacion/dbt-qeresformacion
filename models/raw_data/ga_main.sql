SELECT
  date,
  _fivetran_id,
  profile,
  _fivetran_synced,
  campaign,
  hostname,
  landing_page_path,
  case when medium='(none)' then null else medium end as medium,
  source,
  transaction_id,
  transaction_revenue,
  transactions
FROM
  `beaming-crowbar-330609.raw_google_analytics.google_analytics_custom_report`