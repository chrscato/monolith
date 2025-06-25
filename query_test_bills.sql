-- Query to examine the ProviderBill records for the test bills
SELECT 
    id,
    status,
    action,
    bill_paid,
    patient_name,
    patient_dob,
    patient_zip,
    billing_provider_name,
    billing_provider_address,
    billing_provider_tin,
    billing_provider_npi,
    total_charge,
    patient_account_no,
    created_at,
    updated_at
FROM ProviderBill 
WHERE id IN (
    '21a240f7-b72f-42ca-8991-70f1ce4296e8',
    '23d9dc5f-e93f-44f9-92b1-6b2ccc3bca79',
    '2cc11e49-abb7-4c7c-bb5c-ccf4d50dd6aa',
    '3722a3c6-d223-4bca-a8dc-46ab06a60866',
    '38c39001-3dff-407d-979e-0d67c5ab2742'
)
ORDER BY created_at;

-- Query to examine the BillLineItem records for the test bills
SELECT 
    bli.id,
    bli.provider_bill_id,
    bli.cpt_code,
    bli.modifier,
    bli.units,
    bli.charge_amount,
    bli.allowed_amount,
    bli.decision,
    bli.reason_code,
    bli.date_of_service,
    bli.place_of_service,
    bli.diagnosis_pointer,
    bli.created_at
FROM BillLineItem bli
WHERE bli.provider_bill_id IN (
    '21a240f7-b72f-42ca-8991-70f1ce4296e8',
    '23d9dc5f-e93f-44f9-92b1-6b2ccc3bca79',
    '2cc11e49-abb7-4c7c-bb5c-ccf4d50dd6aa',
    '3722a3c6-d223-4bca-a8dc-46ab06a60866',
    '38c39001-3dff-407d-979e-0d67c5ab2742'
)
ORDER BY bli.provider_bill_id, bli.id;

-- Summary query showing bill counts and line item counts
SELECT 
    pb.id,
    pb.status,
    pb.billing_provider_name,
    pb.total_charge,
    COUNT(bli.id) as line_item_count,
    SUM(bli.charge_amount) as line_items_total
FROM ProviderBill pb
LEFT JOIN BillLineItem bli ON pb.id = bli.provider_bill_id
WHERE pb.id IN (
    '21a240f7-b72f-42ca-8991-70f1ce4296e8',
    '23d9dc5f-e93f-44f9-92b1-6b2ccc3bca79',
    '2cc11e49-abb7-4c7c-bb5c-ccf4d50dd6aa',
    '3722a3c6-d223-4bca-a8dc-46ab06a60866',
    '38c39001-3dff-407d-979e-0d67c5ab2742'
)
GROUP BY pb.id, pb.status, pb.billing_provider_name, pb.total_charge
ORDER BY pb.created_at;

-- Query to check for the specific NPI validation issues
SELECT 
    id,
    billing_provider_name,
    billing_provider_npi,
    CASE 
        WHEN billing_provider_npi IS NULL THEN 'NULL'
        WHEN LENGTH(billing_provider_npi) != 10 THEN 'Wrong length'
        WHEN billing_provider_npi NOT REGEXP '^[0-9]+$' THEN 'Contains non-digits'
        ELSE 'Valid'
    END as npi_validation_status
FROM ProviderBill 
WHERE id IN (
    '21a240f7-b72f-42ca-8991-70f1ce4296e8',
    '23d9dc5f-e93f-44f9-92b1-6b2ccc3bca79',
    '2cc11e49-abb7-4c7c-bb5c-ccf4d50dd6aa',
    '3722a3c6-d223-4bca-a8dc-46ab06a60866',
    '38c39001-3dff-407d-979e-0d67c5ab2742'
)
ORDER BY id; 