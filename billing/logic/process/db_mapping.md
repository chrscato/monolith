Key Table Relationships

ProviderBill ↔ Orders

ProviderBill.claim_id links to Orders.Order_ID
Used to associate a provider bill with an order
Key patient information is in both tables and can be cross-validated


BillLineItem ↔ order_line_items

BillLineItem.provider_bill_id + BillLineItem.cpt_code must be compared against order_line_items.Order_ID + order_line_items.CPT
Each line item on a bill needs to be matched with a corresponding ordered service


CPT Code Validation

dim_proc table stores category and subcategory information for CPT codes
Used for checking clinical equivalence when exact CPT codes don't match


Rate Validation

ppo table contains in-network rates by provider TIN and CPT code
ota table contains out-of-network rates by order and CPT code