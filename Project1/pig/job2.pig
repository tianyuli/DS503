customers = LOAD '/user/hadoop/input/Customers.csv' USING PigStorage(',') AS (id, name, age, gender, countryCode, salary);
transactions = LOAD '/user/hadoop/input/Transactions.csv' USING PigStorage(',') AS (transID, custID, transTotal, transNumItems, transDesc);
A = JOIN transactions by custID, customers by id USING 'replicated';
B = GROUP A BY (customers::id, customers::name, customers::salary);
C = FOREACH B GENERATE group.id, group.name, group.salary, COUNT(A) AS NumOfTransactions, 
	SUM(A.transTotal) AS TotalSum, MIN(A.transNumItems) AS MinItems;
STORE C into '/user/hadoop/output/job2';
