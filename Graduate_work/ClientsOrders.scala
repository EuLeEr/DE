@nowarn
//spark read data from cvs file

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType, DateType}
import org.apache.spark.sql.functions._





object ClientsOrders extends App {
  val spark = SparkSession
        .builder()
        .appName("Spark Data Showcases")
        .config("spark.jars", "C:\\spark-3.3.1-bin-hadoop3\\jars\\postgresql-42.5.1.jar") 
        .config("spark.master", "local")
        .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  // import spark.implicits._

  // Create schema for the CSV file
  val schemaClient = StructType(Array(
    StructField("ClientId", IntegerType, true),
    StructField("ClientName", StringType, true),
    StructField("Type", StringType, true),
    StructField("Form", StringType, true),
    StructField("RegisterDate", DateType , true))
  )

  val schemaOperation = StructType(Array(
    StructField("AccountDb", IntegerType, true),
    StructField("AccountCR", IntegerType, true),
    StructField("DateOp", DateType, true),
    StructField("Amount", StringType, true),
    StructField("Currency", StringType, true), 
    StructField("Comment", StringType, true))
  )

  val schemaAccount = StructType(Array(
    StructField("AccountId", IntegerType, true),
    StructField("AccountNum", StringType, true),
    StructField("ClientId", IntegerType, true),
    StructField("DateOpen", DateType, true)
    )
  )

  val schemaCurrency = StructType(Array(
    StructField("Currency", StringType, true),
    StructField("Rate", StringType, true),
    StructField("RateDate", DateType, true)
    )
  )

  
  val dfClient = spark.read.format("csv")
    .option("header", "true") //reading the headers
    .option("delimiter", ";")
  
    .schema(schemaClient) //passing the schema
    .load("Data/Clients.csv")

  var dfvOperation =   spark.read.format("csv")
    .option("header", "true") //reading the headers
    .option("delimiter", ";")
    .option("encoding","utf-8")
    .schema(schemaOperation) //passing the schema
    .load("Data/Operation.csv")

  val dfOperation = dfvOperation.withColumn("Amount", regexp_replace(col("Amount"),",",".").cast(FloatType))

    //dfOperation.write.mode("overwrite").csv("c:/Data/Operation1.parguet")  
  var dfvRate = spark.read.format("csv")
    .option("header", "true") //reading the headers
    .option("delimiter", ";")
    .option("encoding","utf-8")
    .schema(schemaCurrency) //passing the schema
    .load("Data/Rate.csv")
    

  val dfRate = dfvRate.withColumn("Rate", regexp_replace(col("Rate"),",",".").cast(FloatType))
   // dfRate.write.mode("overwrite").parquet("c:/Data/Rate.parguet")    
  val dfAccount = spark.read.format("csv")
    .option("header", "true") //reading the headers
    .option("delimiter", ";")
    .option("encoding","utf-8")
    .schema(schemaAccount) //passing the schema
    .load("Data/Account.csv")
// Data Quality 

  // Проверяем таблицу клиентов на наличие дубликатов
  val ClientDup = dfClient.groupBy("ClientId").count().filter("count > 1").count()
  println("Quantity dublet in data Client:  " + ClientDup)
  // Проверяем таблицу счетов на наличие дубликатов
  val AccountDup = dfAccount.groupBy("AccountId").count().filter("count > 1").count()
  println("Quantity dublet in data Account: " + AccountDup)  
  // Проверяем таблицу операций на пустые значения в колонках
  val OperationNull = dfOperation.filter("AccountDb is null or AccountCR is null or DateOp is null or Amount is null or Currency is null or Comment is null").count()
  println("Quantity null in data Operation: " + OperationNull)
  // Проверяем таблицу операций на пустых количеств
  val OperationZero = dfOperation.filter("Amount = 0").count()
  println("Quantity zero in data Operation: " + OperationZero)
  // Проверяем таблицу клиентов на формы собственности и типы клиентов
  val ClientType = dfClient.filter("Type is null or Form is null").count()
  println("Quantity null in data Client: " + ClientType)
  // Группируем таблицу клиентов по типам и формам собственности
  val ClientGroup = dfClient.groupBy("Type","Form").count()
  ClientGroup.show()
  //ClientGroup.write.mode("overwrite").csv("Data/ClientGroup.csv")
  
  // select from postgresql table
  // dataframe.select("id","user_id","fio","dbd","dpa").write.format("jdbc")\
  //   .option("url", "jdbc:postgresql://0.0.0.0:5432/test") \
  //   .option("driver", "org.postgresql.Driver").option("dbtable", "lk2") \
  //   .option("user", "postgres").option("password", "postgres").save()
//   val dfLists = spark.createDataFrame().select("listName","Content").read.format("jdbc")
//     .option("url", "jdbc:postgresql:////0.0.0.0:5432/test") 
//     .option("driver", "org.postgresql.Driver")
//     .option("dbtable", "lists") 
//     .option("user", "postgres")
//     .option("password", "postgres")
//     .load()
//   //spark.read.jdbc(url, "lists", properties).collect(), schemaLists)

// //Scala split string "%vin%,%viн:%, %fоrd%, %мiтsuвisнi%,  %нissан%, %sсанiа%, %вмw%, %реugеот%,  %vоlкswаgен%,  %sкоdа%,  %rover%    into array of string
// val dfLists = dfLists.withColumn("Content", split(col("Content"), ","))
// dfLists.show()
//Витрина _corporate_payments_. Строится по каждому уникальному счету (AccountDB  и AccountCR) из таблицы Operation. Ключ партиции CutoffDt
val dfRateCurrent = dfRate.groupBy("Currency").agg(max("RateDate").alias("RateDate")).join(dfRate, Seq("Currency","RateDate")).select("Currency","RateDate","Rate")
dfRateCurrent.createOrReplaceTempView("RateCurrent");
dfAccount.createOrReplaceTempView("Accounts");
dfClient.createOrReplaceTempView("Clients");
dfOperation.createOrReplaceTempView("CorporatePayments")

val dfAccountPaymentsCurrency = spark.sql("select AccountDB, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as PaymentMnt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) group by  CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfAccountPayments = dfAccountPaymentsCurrency.groupBy("AccountDB", "CutoffDt" ).agg(sum("PaymentMnt").as("PaymentMnt"))
val dfAccountEnrollementsCurrency = spark.sql("select AccountCR, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as EnrollementAmt  from CorporatePayments join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) group by  CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfAccountEnrollement = dfAccountEnrollementsCurrency.groupBy("AccountCR", "CutoffDt" ).agg(sum("EnrollementAmt").as("EnrollementAmt"))
//TaxAmt = Сумму операций, где счет клиента указан в дебете, и счет кредита 40702
val dfTaxAmtCurrency = spark.sql("select AccountDB, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as TaxAmt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) where AccountCR = 40702 group by  CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfTaxAmt = dfTaxAmtCurrency.groupBy("AccountDB", "CutoffDt" ).agg(sum("TaxAmt").as("TaxAmt"))
//ClearAmt = Сумма операций, где счет клиента указан в кредите, и счет дебета 40802
val dfClearCurrency = spark.sql("select AccountCR, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as ClearAmt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) where AccountDB = 40802 group by  CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfClearAmt = dfClearCurrency.groupBy("AccountCR", "CutoffDt" ).agg(sum("ClearAmt").as("ClearAmt"))
//CarsAmt Сумма операций, где счет клиента указан в дебете проводки и назначение платежа не содержит слов по маскам Списка 1
val dfCarsAmtCurrency = spark.sql("select AccountDB, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as CarsAmt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) where  (Comment not like '%Автомобиль%' and Comment not like '%Авто%') group by  CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfCarsAmt = dfCarsAmtCurrency.groupBy("AccountDB", "CutoffDt" ).agg(sum("CarsAmt").as("CarsAmt"))
//FoodAmt Сумма операций, где счет клиента указан в кредите проводки и назначение платежа содержит слова по Маскам Списка 2
val dfFoodAmtCurrency = spark.sql("select AccountCR, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as FoodAmt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) where  (Comment like '%Еда%' or Comment like '%алко%') group by  CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfFoodAmt = dfFoodAmtCurrency.groupBy("AccountCR", "CutoffDt" ).agg(sum("FoodAmt").as("FoodAmt"))
//FLAmt Сумма операций с физ. лицами. Счет клиента указан в дебете проводки, а клиент в кредите проводки – ФЛ.
var dfFLAmtCurrency = spark.sql("select AccountDB, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as FLAmt , RateCurrent.Rate, CorporatePayments.Currency from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) join Accounts on  AccountCR = AccountID  join Clients on Accounts.ClientId = Clients.ClientId where Clients.Type = 'Ф' group by  CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfFLAmt = dfFLAmtCurrency.groupBy("AccountDB","CutoffDt").agg(round(sum("FLAmt"),2).as("FLAmt"))
// Full corporate payments
dfAccountPayments.show()
dfFLAmt.show()
val dfCorporatePayments = dfAccountPayments.join(dfTaxAmt, Seq("AccountDB", "CutoffDt"), "outer").join(dfCarsAmt, Seq("AccountDB", "CutoffDt"), "outer").join(dfFLAmt, Seq("AccountDB", "CutoffDt"), "outer")
//val dfCorporatePayments = dfTaxAmt.join(dfClearAmt, Seq("AccountCR", "CutoffDt"), "outer").join(dfCarsAmt, Seq("AccountDB", "CutoffDt"), "outer").join(dfFoodAmt, Seq("AccountCR", "CutoffDt"), "outer").join(dfFLAmt, Seq("AccountDB", "CutoffDt"), "outer")
dfCorporatePayments.show()
//val dfCorporatePayments = dfAccountPayments.join(dfAccountEnrollement, Seq("CutoffDt","Currency"), "outer")
//dfCorporatePayments.write.mode("overwrite").partitionBy("CutoffDt").parquet("Data/CorporatePayments.parquet")

//dfFLAmt.show()
 
}






