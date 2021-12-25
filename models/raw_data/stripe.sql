SELECT
  _fivetran_synced,
  id,
  amount,
  available_on,
  connected_account_id,
  created,
  currency,
  hp.description,
  exchange_rate,
  fee,
  net,
  payout_id,
  hp.type,
  hp.source,
  hp.status
FROM
  `beaming-crowbar-330609.stripe.balance_transaction` hp