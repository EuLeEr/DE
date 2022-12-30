


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType, DateType}
import org.apache.spark.sql.functions._





object ClientsOrders extends App {
  val spark = SparkSession
        .builder()
        .appName("Spark Data Showcases")
        .config("spark.jars", "C:\\spark-3.3.1-bin-hadoop3\\jars\\postgresql-42.5.1.jar") 
//        .config("spark.driver.bindAddress","localhost") 
        .config("spark.master", "local")
//        .config("spark.ui.port","4040")
        .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._      
  spark.sparkContext.setLogLevel("WARN") 
    
  // val dfLists = spark.read.format("jdbc")
  //   .option("url", "jdbc:postgresql://0.0.0.0:5432/Graduate")
  //   .option("driver", "org.postgresql.Driver")
  //   .option("dbtable", "lists") 
  //   .option("user", "postgres")
  //   .option("password", "postgres")
  //   .load()
  
  // val dfListsplit = dfLists.withColumn("Content", split(col("Content"), ","))
  // dfLists.show()
  // dfListsplit.show()
  // select from postgresql table
  // dataframe.select("id","user_id","fio","dbd","dpa").write.format("jdbc")\
  //   .option("url", "jdbc:postgresql://0.0.0.0:5432/test") \
  //   .option("driver", "org.postgresql.Driver").option("dbtable", "lk2") \
  //   .option("user", "postgres").option("password", "postgres").save()

//   //spark.read.jdbc(url, "lists", properties).collect(), schemaLists)

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
    .load("Data/Source/Clients.csv")

  var dfvOperation =   spark.read.format("csv")
    .option("header", "true") //reading the headers
    .option("delimiter", ";")
    .option("encoding","utf-8")
    .schema(schemaOperation) //passing the schema
    .load("Data/Source/Operation.csv")

  val dfOperation = dfvOperation.withColumn("Amount", regexp_replace(col("Amount"),",",".").cast(FloatType))

  
  var dfvRate = spark.read.format("csv")
    .option("header", "true") //reading the headers
    .option("delimiter", ";")
    .option("encoding","utf-8")
    .schema(schemaCurrency) //passing the schema
    .load("Data/Source/Rate.csv")
    

  val dfRate = dfvRate.withColumn("Rate", regexp_replace(col("Rate"),",",".").cast(FloatType))
     
  val dfAccount = spark.read.format("csv")
    .option("header", "true") //reading the headers
    .option("delimiter", ";")
    .option("encoding","utf-8")
    .schema(schemaAccount) //passing the schema
    .load("Data/Source/Account.csv")

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
  //ClientGroup.show()
  //ClientGroup.write.mode("overwrite").csv("Data/ClientGroup.csv")
  




//Витрина _corporate_payments_. Строится по каждому уникальному счету (AccountDB  и AccountCR) из таблицы Operation. Ключ партиции CutoffDt
val dfRateCurrent = dfRate.groupBy("Currency").agg(max("RateDate").alias("RateDate")).join(dfRate, Seq("Currency","RateDate")).select("Currency","RateDate","Rate")
dfRateCurrent.createOrReplaceTempView("RateCurrent");
dfAccount.createOrReplaceTempView("Accounts");
dfClient.createOrReplaceTempView("Clients");
dfOperation.createOrReplaceTempView("CorporatePayments")

val dfAccountPaymentsCurrency = spark.sql("select AccountDB, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as PaymentMnt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) group by  CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfAccountPayments = dfAccountPaymentsCurrency.groupBy("AccountDB", "CutoffDt" ).agg(round(sum("PaymentMnt"),2).as("PaymentMnt")).sort("AccountDB")
val dfAccountEnrollementsCurrency = spark.sql("select AccountCR, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as EnrollementAmt  from CorporatePayments join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) group by  CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfAccountEnrollement = dfAccountEnrollementsCurrency.groupBy("AccountCR", "CutoffDt" ).agg(round(sum("EnrollementAmt"),2).as("EnrollementAmt"))
//TaxAmt = Сумму операций, где счет клиента указан в дебете, и счет кредита 40702
val dfTaxAmtCurrency = spark.sql("select AccountDB, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as TaxAmt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) where AccountCR = 40702 group by  CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfTaxAmt = dfTaxAmtCurrency.groupBy("AccountDB", "CutoffDt" ).agg(round(sum("TaxAmt"),2).as("TaxAmt"))
//ClearAmt = Сумма операций, где счет клиента указан в кредите, и счет дебета 40802
val dfClearCurrency = spark.sql("select AccountCR, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as ClearAmt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) where AccountDB = 40802 group by  CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfClearAmt = dfClearCurrency.groupBy("AccountCR", "CutoffDt" ).agg(round(sum("ClearAmt"),2).as("ClearAmt"))
//CarsAmt Сумма операций, где счет клиента указан в дебете проводки и назначение платежа не содержит слов по маскам Списка 1
val dfCarsAmtCurrency = spark.sql("select AccountDB, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as CarsAmt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) where  (Comment not like '%Автомобиль%' and Comment not like '%Авто%') group by  CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfCarsAmt = dfCarsAmtCurrency.groupBy("AccountDB", "CutoffDt" ).agg(round(sum("CarsAmt"),2).as("CarsAmt"))
//FoodAmt Сумма операций, где счет клиента указан в кредите проводки и назначение платежа содержит слова по Маскам Списка 2
val dfFoodAmtCurrency = spark.sql("select AccountCR, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as FoodAmt  from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) where  (Comment like '%Еда%' or Comment like '%алко%') group by  CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfFoodAmt = dfFoodAmtCurrency.groupBy("AccountCR", "CutoffDt" ).agg(round(sum("FoodAmt"),2).as("FoodAmt"))
//FLAmt Сумма операций с физ. лицами. Счет клиента указан в дебете проводки, а клиент в кредите проводки – ФЛ.
var dfFLAmtCurrency = spark.sql("select AccountDB, DateOp as CutoffDt , round(sum(Amount * RateCurrent.Rate),2) as FLAmt , RateCurrent.Rate, CorporatePayments.Currency from CorporatePayments  join RateCurrent on (RateCurrent.Currency = CorporatePayments.Currency and RateCurrent.RateDate <= CorporatePayments.DateOp) join Accounts on  AccountCR = AccountID  join Clients on Accounts.ClientId = Clients.ClientId where Clients.Type = 'Ф' group by  CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")
val dfFLAmt = dfFLAmtCurrency.groupBy("AccountDB","CutoffDt").agg(round(sum("FLAmt"),2).as("FLAmt")).sort("AccountDB")


// Full corporate payments

val dfCorporatePayments = dfAccountPayments.join(dfTaxAmt, Seq("AccountDB", "CutoffDt"), "outer").join(dfCarsAmt, Seq("AccountDB", "CutoffDt"), "outer").join(dfFLAmt, Seq("AccountDB", "CutoffDt"), "outer")
val dfCorporatePaymentsCR = dfAccountEnrollement.join(dfClearAmt, Seq("AccountCR", "CutoffDt"), "outer").join(dfFoodAmt, Seq("AccountCR", "CutoffDt"), "outer")

dfCorporatePayments.createOrReplaceTempView("CorporatePaymentsDB")
dfCorporatePaymentsCR.createOrReplaceTempView("CorporatePaymentsCR")
val dfCorporatePaymentsWithClient = spark.sql("select CorporatePaymentsDB.AccountDB as AccountId, Accounts.ClientId, CorporatePaymentsDB.CutoffDt, CorporatePaymentsDB.PaymentMnt , CorporatePaymentsDB.TaxAmt, CorporatePaymentsDB.CarsAmt, CorporatePaymentsDB.FLAmt from CorporatePaymentsDB  join Accounts on CorporatePaymentsDB.AccountDB = Accounts.AccountId")
val dfCorporatePaymentsCRWithClient = spark.sql("select CorporatePaymentsCR.AccountCR as AccountId, Accounts.ClientId, CorporatePaymentsCR.CutoffDt, CorporatePaymentsCR.EnrollementAmt, CorporatePaymentsCR.ClearAmt, CorporatePaymentsCR.FoodAmt from CorporatePaymentsCR  join Accounts on CorporatePaymentsCR.AccountCR = Accounts.AccountId")
val dfCorporatePaymentsShowCaseDisOrder = dfCorporatePaymentsWithClient.join(dfCorporatePaymentsCRWithClient, Seq("AccountId", "ClientId" , "CutoffDt"), "outer")
dfCorporatePaymentsShowCaseDisOrder.createOrReplaceTempView("CorporatePaymentsShowCase")
val dfCorporatePaymentsShowCase = spark.sql("select AccountId, ClientId, CutoffDt, PaymentMnt, EnrollementAmt, TaxAmt, CarsAmt, FLAmt,  ClearAmt, FoodAmt from CorporatePaymentsShowCase order by AccountId, CutoffDt")
dfCorporatePaymentsShowCase.write.mode("overwrite").partitionBy("CutoffDt").parquet("Data/Витрина _corporate_payments_.parquet")


// Витрина _corporate_account_. Строится по каждому уникальному счету из таблицы Operation на заданную дату расчета. Ключ партиции CutoffDt
// AccountId - Счет клиента
// AccountNum - Номер счета
// DateOpen - Дата открытия счета
// ClientId - Код клиента
// ClientName - Наименование клиента
// TotalMnt - Сумма операций по счету = PaymentMnt + EnrollementAmt
// CutoffDt - Дата расчета

val dfCorporateAccount = spark.sql("select CorporatePaymentsShowCase.AccountId, Accounts.AccountNum, Accounts.DateOpen, CorporatePaymentsShowCase.ClientId, Clients.ClientName,  (case when PaymentMnt is null then 0 else PaymentMnt end + case when EnrollementAmt is null then 0 else EnrollementAmt end) as TotalMnt, CutoffDt from CorporatePaymentsShowCase join Clients on CorporatePaymentsShowCase.ClientID = Clients.ClientID join Accounts on CorporatePaymentsShowCase.AccountId = Accounts.AccountID order  by AccountId, CutoffDt")

dfCorporateAccount.createOrReplaceTempView("CorporateAccount")
dfCorporateAccount.write.mode("overwrite").partitionBy("CutoffDt").parquet("Data/Витрина _corporate_account_.parquet")

// Витрина _corporate_info_. Строится по каждому уникальному клиенту из таблицы Operation. Ключ партиции CutoffDt
// ClientId - Код клиента
// ClientName - Наименование клиента
// Type - Тип клиента (Юр. лицо, Физ. лицо)
// Form - Форма собственности (ООО, ОАО, ЗАО, ИП)
// RegisterDate - Дата регистрации
// TotalMnt - Сумма операций по всем счетам клиент. Считается как сумма corporate_account.total_amt по всем счетам.

val dfCorporateInfo = spark.sql("select CorporateAccount.ClientId, CorporateAccount.ClientName, Type, Form, RegisterDate, sum(TotalMnt) as TotalMnt, CutoffDt from CorporateAccount join Clients on CorporateAccount.ClientId = Clients.ClientId group by CorporateAccount.ClientId, CorporateAccount.ClientName, Type, Form, RegisterDate, CutoffDt order by CorporateAccount.ClientId, CutoffDt")

dfCorporateInfo.write.mode("overwrite").partitionBy("CutoffDt").parquet("Data/Витрина _corporate_info_.parquet")



}






