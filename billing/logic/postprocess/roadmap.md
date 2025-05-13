## roadmap for postprocessing logic

--first i want the directory to be similar to process and preprocess. have utils, jobs etc

--
the first load is to use the db and pull the bills where the status is approved and not already paid yet which is bill_paid field set to null all under the ProviderBills table

--
then we do a check to bring in all the relevant data using the db_utils.py

join in all the other tables like orders, orders_line_items, Providers, ProviderBills, BillLineItem, ppo, FeeSchedule, ota

do you need the join logic for that? can you help me build a .yaml that can be referenced?


--
then after that we have checks to ensure all the datapoints are present and valid. build a utils scaffold for me there I will improve it

--
then we proceed with filling out a docx template with placeholders of the key datapoints and build a EOBR for the given bill

--
then we create an excel file that fits a certain template, more to come there

--
and we add the excel template data to another excel that is a historical log


--
then we mark the provider bill paid in a few different places, across 3 tables, more details to come there

--
these are meant to run in batches upon my approval, so need a main.py that runs it all together