## Process

-- this process is to take a bill that is under the status of select * from ProviderBill where status = MAPPED

--it is to review a bill for validity and take it to the next processing step

--this is a comparison of the 2 sources orders and orders_line_items and ProviderBill and BillLineItem layman (FileMaker vs HCFA)

-- the mapping is such that we can get the comparison going 


CREATE TABLE "BillLineItem" (
	"id"	INTEGER,
	"provider_bill_id"	TEXT,
	"cpt_code"	TEXT,
	"modifier"	TEXT,
	"units"	INTEGER,
	"charge_amount"	REAL,
	"allowed_amount"	REAL,
	"decision"	TEXT,
	"reason_code"	TEXT,
	"date_of_service"	TEXT,
	"place_of_service"	TEXT,
	"diagnosis_pointer"	TEXT,
	PRIMARY KEY("id" AUTOINCREMENT),
	FOREIGN KEY("provider_bill_id") REFERENCES "ProviderBill"("id")
)


CREATE TABLE ProviderBill (
        id TEXT PRIMARY KEY,
        claim_id TEXT,
        uploaded_by TEXT,
        source_file TEXT,
        status TEXT,
        last_error TEXT,
        created_at TEXT
    , patient_name TEXT, patient_dob TEXT, patient_zip TEXT, billing_provider_name TEXT, billing_provider_address TEXT, billing_provider_tin TEXT, billing_provider_npi TEXT, total_charge REAL, patient_account_no TEXT, action TEXT)


CREATE TABLE "orders" (
	"Order_ID"	TEXT,
	"FileMaker_Record_Number"	TEXT,
	"Patient_Address"	TEXT,
	"Patient_City"	TEXT,
	"Patient_State"	TEXT,
	"Patient_Zip"	TEXT,
	"Patient_Injury_Date"	TEXT,
	"Patient_Injury_Description"	TEXT,
	"Patient_DOB"	TEXT,
	"Patient_Last_Name"	TEXT,
	"Patient_First_Name"	TEXT,
	"PatientName"	TEXT,
	"PatientPhone"	TEXT,
	"Referring_Physician"	TEXT,
	"Referring_Physician_NPI"	TEXT,
	"Assigning_Company"	TEXT,
	"Assigning_Adjuster"	TEXT,
	"Claim_Number"	TEXT,
	"Order_Type"	TEXT,
	"Jurisdiction_State"	TEXT,
	"created_at"	TIMESTAMP,
	"updated_at"	TIMESTAMP,
	"is_active"	REAL,
	"bundle_type"	TEXT,
	"provider_id"	TEXT,
	"provider_name"	NUMERIC,
	"BILLS_PAID"	INTEGER
, "FULLY_PAID"	TEXT, "BILLS_REC"	INTEGER)



CREATE TABLE "order_line_items" (
"id" TEXT,
  "Order_ID" TEXT,
  "DOS" TEXT,
  "CPT" TEXT,
  "Modifier" TEXT,
  "Units" TEXT,
  "Description" TEXT,
  "Charge" TEXT,
  "line_number" TEXT,
  "created_at" TIMESTAMP,
  "updated_at" TIMESTAMP,
  "is_active" TEXT,
  "BR_paid" TEXT,
  "BR_rate" TEXT,
  "EOBR_doc_no" TEXT,
  "HCFA_doc_no" TEXT,
  "BR_date_processed" TEXT
, "BILLS_PAID"	INTEGER)



--a place to compare the category and subcategory between the 2 sources
CREATE TABLE dim_proc (
            id INTEGER PRIMARY KEY,
            proc_cd TEXT,
            modifier TEXT,
            proc_desc TEXT,
            category TEXT,
            subcategory TEXT
        )


-- a place to pull the in network rates

CREATE TABLE "ppo" (
"id" TEXT,
  "RenderingState" TEXT,
  "TIN" TEXT,
  "provider_name" TEXT,
  "proc_cd" TEXT,
  "modifier" TEXT,
  "proc_desc" TEXT,
  "proc_category" TEXT,
  "rate" TEXT
)


-- a place to pull the out of network rates

CREATE TABLE "ota" (
"ID_Order_PrimaryKey" TEXT,
  "CPT" TEXT,
  "modifier" TEXT,
  "rate" TEXT
)


-- a place to pull the provider information

CREATE TABLE "providers" (
"_g_Dashboard_Provider_Diary" REAL,
  "_g_Dashboard_Provider_Status" REAL,
  "Address 1 Full" TEXT,
  "Address Line 1" TEXT,
  "Address Line 2" TEXT,
  "All" TEXT,
  "Angiography" TEXT,
  "Arthrogram" TEXT,
  "Billing Address 1" TEXT,
  "Billing Address 2" TEXT,
  "Billing Address City" TEXT,
  "Billing Address Postal Code" TEXT,
  "Billing Address State" TEXT,
  "Billing Name" TEXT,
  "Bone Density" TEXT,
  "Breast MRI" TEXT,
  "City" TEXT,
  "Contract Date" TEXT,
  "Contract Date Renewal" TEXT,
  "Country" TEXT,
  "CreatedBy" TEXT,
  "CreationTimestamp" TEXT,
  "CT" TEXT,
  "CT W" TEXT,
  "CT WO" TEXT,
  "CT WWO" TEXT,
  "CTA" TEXT,
  "DBA Name Billing Name" TEXT,
  "Description" TEXT,
  "Dexa" TEXT,
  "Diary Date" TEXT,
  "distance" TEXT,
  "Echo" TEXT,
  "EKG" TEXT,
  "Email" TEXT,
  "EMG" TEXT,
  "Fax Number" TEXT,
  "Fluroscopy" TEXT,
  "ForeignKey" TEXT,
  "g_lat" TEXT,
  "g_lon" TEXT,
  "High Field Closed MRI" TEXT,
  "ImportIndex" TEXT,
  "lat" TEXT,
  "Latitude" TEXT,
  "Location" TEXT,
  "lon" TEXT,
  "Longitude" TEXT,
  "Magnet Strength Closed Tensile High Field" TEXT,
  "Magnet Strength MRA Tensil" TEXT,
  "Magnet Strength Open Tensile" TEXT,
  "Mammo" TEXT,
  "ModificationTimestamp" TEXT,
  "ModifiedBy" TEXT,
  "MRA" TEXT,
  "MRI 1.5T" TEXT,
  "MRI 3.0T" TEXT,
  "MRI Open" TEXT,
  "MRI W" TEXT,
  "MRI Wide Bore" TEXT,
  "MRI WO" TEXT,
  "MRI WWO" TEXT,
  "Name" TEXT,
  "Need OTA" TEXT,
  "NPI" TEXT,
  "NUC Medicine" TEXT,
  "OrderForeignKey" TEXT,
  "Other Service" TEXT,
  "Parent" TEXT,
  "ParentKey" TEXT,
  "ParentSearch" TEXT,
  "PET" TEXT,
  "Phone" TEXT,
  "Postal Code" TEXT,
  "Pricing Group" TEXT,
  "Primary" TEXT,
  "PrimaryKey" TEXT,
  "Provider Network" TEXT,
  "Provider Status" TEXT,
  "Provider Type" TEXT,
  "Record Status" TEXT,
  "ServicesProvided" TEXT,
  "State" TEXT,
  "Status" TEXT,
  "TIN" TEXT,
  "US" TEXT,
  "Website" TEXT,
  "Wide Bore" TEXT,
  "xlink" TEXT,
  "Xray" TEXT
)



so please first clarify all the mapping and ensure they map to one another