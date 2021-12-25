SELECT
  id,
  amount,
  bank_id,
  contact_id,
  pt.date,
  pt.DESC,
  document_id,
  document_type
FROM
  `beaming-crowbar-330609.google_cloud_function.transaction` pt
  where document_type='payroll'
