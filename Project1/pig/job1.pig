customers = LOAD '/user/hadoop/input/Customers.csv' USING PigStorage(',') AS (id, name, age, gender, countryCode, salary);
transactions = LOAD '/user/hadoop/input/Transactions.csv' USING PigStorage(',') AS (transId, custId, transTotal, transNumItems, transDesc);
A = JOIN customers BY id, transactions BY custId;
B = GROUP A BY (customers::id, customers::name);
C = FOREACH B GENERATE group.name, COUNT(A) as cnt;

minTrans = LIMIT (ORDER C by cnt ASC) 1;
rst = FILTER C by cnt == minTrans.cnt;

store rst into '/user/hadoop/output/job1';
