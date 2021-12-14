SELECT  
t.name,
id as client_id,
email,
phone,
type,
JSON_VALUE(bill_address,'$.address') as address,
JSON_VALUE(bill_address,'$.city') as city,
JSON_VALUE(bill_address,'$.countryCode') as countryCode,
JSON_VALUE(bill_address,'$.postalCode') as postalCode
FROM 
(SELECT contact_name as name, from `beaming-crowbar-330609.google_cloud_function_documents.transaction` GROUP BY contact_name) t 
left join `beaming-crowbar-330609.holded_contacts.transaction` c on c.name=t.name
order by name