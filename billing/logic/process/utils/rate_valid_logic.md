process to build rate_validation logic functions in rate_validation.py and how to implement them in main.py

--
this is a check to perform after the checks already in place on process/main.py

--
this check is for when it passes all the preceeding checks in process/main.py

--
first check is to see if the given provider associated with the order is 'In Network' or 'Out of Network'

that can be found def get_provider_details from db_queries.py where the Provider PrimaryKey is joined from Order provider_id and order is pulled from claim_id on the given providerbill record.

--
if the "Provider Network" is "In Network" then use the def get_in_network_rate from db_queries.py

--
if the "Provider Network" is "Out of Network" then use the def get_out_of_network_rate from db_queries.py

--
this is all based on the ProviderBill line item cpt_code rate not the order_line_item cpt_code

--
the precheck is to see for any ancillaries and automatically give it a $0 rate

--
