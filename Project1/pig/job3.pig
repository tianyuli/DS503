customers = LOAD '/user/hadoop/input/Customers.csv' USING PigStorage(',')
    AS (customerId:int, name:chararray, age:int, gender:chararray, Code: int, salary:float);
A = GROUP customers BY Code;
B = FOREACH A GENERATE group, COUNT(customers) as cnt;
C = FILTER B BY (cnt > 5000) OR (cnt < 2000);
D = FOREACH C GENERATE group;
store D into '/user/hadoop/output/job3';
