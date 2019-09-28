customers = LOAD '/user/hadoop/input/Customers.csv' USING PigStorage(',') AS (id, name, age, gender, countryCode, salary);
transactions = LOAD '/user/hadoop/input/Transactions.csv' USING PigStorage(',') AS (transID, custID, transTotal, transNumItems, transDesc);

A = filter customers by (age>=10) and (age<20);
g1 = foreach A generate '[10,20)' as range, *;

B = filter customers by (age>=20) and (age<30);
g2 = foreach A generate '[20,30)' as range, *;

C = filter customers by (age>=30) and (age<40) and (gender=='male');
g3 = foreach A generate '[30,40)' as range, *;

D = filter customers by (age>=40) and (age<50) and (gender=='male');
g4 = foreach A generate '[40,50)' as range, *;

E = filter customers by (age>=50) and (age<60) and (gender=='male');
g5 = foreach A generate '[50,60)' as range, *;

F = filter customers by (age>=60) and (age<=70) and (gender=='male');
g6 = foreach A generate '[60, 70]' as range, *;

unioned = union g1, g2, g3, g4, g5, g6;
joined = JOIN transactions by custID, unioned by id;
grouped = GROUP joined BY (unioned::range, unioned::gender);
rst = FOREACH grouped GENERATE group.range, group.gender, MIN(joined.transTotal), MAX(joined.transTotal), AVG(joined.transTotal);


store rst into '/user/hadoop/output/job4';
