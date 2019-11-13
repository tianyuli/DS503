import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val transactionsRDD = sc.textFile("Transactions.csv")
val schemaString = "TransID CustID TransTotal TransNumItems TransDesc"

val fields = schemaString.split(" ").
    map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

val rowRDD = transactionsRDD.
    map(_.split(",")).
    map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4).trim))
val transactionsDF = spark.createDataFrame(rowRDD, schema)
transactionsDF.createOrReplaceTempView("transactions")


val T1 = spark.sql("""
    SELECT *
    FROM transactions
    WHERE TransTotal > 200
    """)
T1.createOrReplaceTempView("T1")
T1.show()


val T1 = spark.sql("""
    SELECT *
    FROM transactions
    WHERE TransTotal > 200
    """)
T1.createOrReplaceTempView("T1")
T1.show()

T2.show()

var T3 = spark.sql("""
    SELECT CustID, count(*) as TransCount
    FROM T1
    GROUP BY CustID
    """)
T3.createOrReplaceTempView("T3")
T3.show()

var T4 = spark.sql("""
    SELECT *
    FROM transactions
    WHERE TransTotal < 600
    """)
T4.createOrReplaceTempView("T4")
T4.show()

var T5 = spark.sql("""
    SELECT CustID, count(*) as TransCount
    FROM T4
    GROUP BY CustID
    """)
T5.createOrReplaceTempView("T5")

var T6 = spark.sql("""
    SELECT T5.CustID
    FROM T5
    JOIN T3
    ON T5.CustID = T3.CustID
    WHERE (T5.TransCount * 5) < T3.TransCount
    GROUP BY T5.CustID
    """)
T6.createOrReplaceTempView("T6")
T6.show()