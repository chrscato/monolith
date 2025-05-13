### def compare_cpt_code logic and status messaging
--
exact match : status = REVIEWED action = apply_rate
--
category match: status = REVIEWED action = apply_rate
--
when bill contains less line items than order but still match what's in order: status = REVIEWED action = apply_rate (apply that to the order, but mark the matching order_line_item record field BILL_REVIEWED with the given ProviderBill id)
--
when bill contains more line items than order (excluding ancillaries): status = REVIEW_FLAG action = address_line_item_mismatch
--
when a bill mismatches entirely with the order: status = REVIEW_FLAG action = complete_line_item_mismatch