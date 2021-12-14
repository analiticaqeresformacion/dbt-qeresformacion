SELECT
 date,
 'Google Ads' as campaign_type,
  campaign_id,
  campaign_name,
  campaign_status,
  clicks,
  null as cpc,
  impressions,
  cost,
FROM
  {{ref('google_ads')}}

UNION ALL 

SELECT
  date,
  'Facebook Ads' as campaign_type,
  campaign_id,
  campaign_name,
  null as campaign_status,
  clicks,
  cpc,
  impressions,
  spend
FROM
  {{ref('facebook_ads')}}