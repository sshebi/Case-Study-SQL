# Case-Study-SQL
Business Hour Mismatch on Food Delivery Platforms
Overview
This project analyzes business hour inconsistencies between two major food delivery platforms to identify operational discrepancies that impact order volume and customer experience.
Problem Statement
Restaurants often display different business hours across delivery platforms, leading to:

Customer confusion and lost orders
Revenue gaps from incorrect availability windows
Operational inefficiencies

Objective
Develop a SQL solution to:

Identify business hour mismatches between Platform A (ground truth) and Platform B
Classify mismatch severity levels
Quantify operational impact

Classification Framework

"In Range": Platform B hours fall within Platform A operating window
"Out of Range with 5 mins difference": Minor discrepancies (acceptable tolerance)
"Out of Range": Significant mismatches requiring immediate attention

Technical Approach
SQL Solution Features

Cross-platform restaurant matching
Time range comparison logic
Mismatch severity classification
Aggregated reporting metrics
