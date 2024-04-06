import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

import scala.util.Random

case class Name(firstName: String,
                lastName: Option[String])

case class User(user_id: String,
                name: Name,
                phone: String,
                account: Account) // Modify User to include a single account

case class Account(account_number: String,
                   account_type: String,
                   bank_name: String,
                   balance: Double,
                   last_transaction: String)

object Main {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("parseJson").setMaster("local")
    val sc = new SparkContext(conf)

    val jsonPath = "C:\\Users\\anish\\RDD_using_faker\\src\\main\\python\\yo_data.json"
    val jsonRDD: RDD[String] = sc.textFile(jsonPath) // Read the JSON file
    val jValueRDD: RDD[JValue] = jsonRDD.map(parse(_)) // Parse the JSON to a JValue

    implicit val formats: DefaultFormats.type = DefaultFormats

    val userDataRDD = jValueRDD.map { jsonValue =>
      val userJson = jsonValue \ "user"
      val userId = (userJson \ "user_id").extract[String]
      val nameJValue = userJson \ "name"
      val firstName = (nameJValue \ "first").extract[String]
      val lastName = (nameJValue \ "last").extractOpt[String]
      val phone = (userJson \ "phone").extract[String]

      // Extract account information
      val accountJson = jsonValue \ "account"
      val acc_no = (accountJson \ "account_number").extract[String]
      val account_type = (accountJson \ "account_type").extract[String]
      val bank_name = (accountJson \ "bank_name").extract[String]
      val balance = (accountJson \ "balance").extract[Double]
      val last_transaction = (accountJson \ "last_transaction").extract[String]

      val account = Account(acc_no, account_type, bank_name, balance, last_transaction)
      User(userId, Name(firstName, lastName), phone, account)
    }

    val userId = "22"

    val distinctUsersList = userDataRDD.map(x => x.user_id).distinct().collect().toList
    println(s"\nList of distinct user_id : \n$distinctUsersList ")
    println("\nTotal distinct user_id = " + distinctUsersList.length )
    println("\nTotal data present = " + userDataRDD.count() )

    val AccountsPerUserRDD: RDD[(String, Account)] = userDataRDD.map(user => (user.user_id, user.account))
    val userAccounts = AccountsPerUserRDD.groupByKey().collectAsMap().take(2)  // HashMap(id -> Seq(accounts) )
    userAccounts.foreach { case (userId, accounts) => // Print each user ID along with their accounts
      println(s"\nUser ID: $userId")
      println("Accounts:")
      accounts.foreach(println)
    }


    println(s"\nBank Details (Bank_name, Account_no, Balance) for userId $userId-  ")
    val bankDetailsForUser = findAccountDetailsForUserId(userId, userDataRDD)
    bankDetailsForUser.foreach(println)

    println("\nList of banks -  ")
    val banksForUser = findBanksForUserId(userId, userDataRDD)
    println(banksForUser)


    val bankCount = countUsersByBankAdvance(userDataRDD)
    println("\nTotal Users per bank -  ")
    bankCount.foreach(println)

    val accountType = filterUsersByAccountType(userDataRDD, "savings").take(2)
    println("\nList of users with savings account -  ")
    accountType.foreach(println)

    val lowestBalance = findUsersWithLowestBalance(userDataRDD)
    println("\nUser with Lowest balance - ")
    lowestBalance.foreach(println)

    val avgBalance = calculateAverageBalance(userDataRDD)
    println(f"\nTotal Average Balance - $avgBalance%.2f")

    val avgByBank = calculateAverageBalanceByBank(userDataRDD)
    println("\nAverage balance per Bank - ")
    avgByBank.foreach { case (bank, avg) =>
      println(f"$bank - $avg%.2f")
    }

    val userWithMultipleAccounts = findUsersWithMultipleAccounts(userDataRDD).take(5)
    println("\nUser with multiple accounts - ")
    userWithMultipleAccounts.foreach(println)

    val totalByAccountTypeAndBank = calculateTotalBalanceByAccountTypeAndBank(userDataRDD)
    println("\nTotal balance as per Account Type and Bank - ")
    totalByAccountTypeAndBank.foreach { case (bank, total) =>
      println(f"$bank - ${total}%.2f")
    }




    val largeTable: RDD[(Long,User)] = userDataRDD.zipWithIndex().map { case (user, index) => ( index + 1, user) }
    val data: List[(Long, String)] = (List((1, "gold"), (10, "silver"), (30, "bronze")))
    val smallTable: RDD[(Long,String)] = sc.parallelize(data)
//    println("\nSmall Table - ")
//    smallTable.foreach(println) // show table values

    val startTime1 = System.nanoTime()
    val myJoin: RDD[(Long, ( User, String))] = largeTable.join(smallTable) // INNER JOIN
    println("\nMy Inner join  - ")
    myJoin.foreach(println)
    val endTime1 = System.nanoTime()
    val duration1 = (endTime1 - startTime1) / 1e6 // Execution time in milliseconds
    println(s"Execution time for myJoin: $duration1 milliseconds")



    val startTime2 = System.nanoTime()
    val smallTableBroadcast = sc.broadcast(smallTable.collectAsMap()) // Broadcast the small table
    val broadcastJoin = largeTable.mapPartitions { iter =>
      val smallTableMap = smallTableBroadcast.value
      iter.flatMap { case (key, value) =>
        smallTableMap.get(key).map((key, value, _))
      }
    }.map { case (key, user, value) =>
      (key, (user.name.firstName, value))
    }
    println("\nBroadcast join  - ")
    broadcastJoin.foreach(println)
    val endTime2 = System.nanoTime()
    val duration2 = (endTime2 - startTime2) / 1e6 // Execution time in milliseconds
    println(s"Execution time for broadcastJoin: $duration2 milliseconds")


    val smallTable2 = sc.parallelize(Seq((1L, "A"), (2L, "B")))
    val collectedSmallTable = smallTable2.collect()
    val smallTableBroadcast1 = sc.broadcast(collectedSmallTable)
    val newBroadcastJoin = largeTable.join(sc.parallelize(smallTableBroadcast1.value))
    println("\n NEW Broadcast join  - ")
    newBroadcastJoin.foreach(println)

    val semiJoin = largeTable.join(smallTable).map {
      case (key, (largeValue, _)) => (key, largeValue)
    }
    println("\nSemi join - ")
    semiJoin.take(20).foreach(println)
    println(semiJoin.count())


    val antiJoin = largeTable.subtractByKey(smallTable)

    println("\nAnti join - ")
    antiJoin.take(20).foreach(println)
    println(antiJoin.count())

    val rightOuterJoin = largeTable.rightOuterJoin(smallTable)
    println("\nRight outer join - ")
    rightOuterJoin.take(20).foreach(println)
    println(rightOuterJoin.count())

    val leftOuterJoin = largeTable.leftOuterJoin(smallTable)
    println("\nLeft outer join - ")
    leftOuterJoin.take(20).foreach(println)
    println(leftOuterJoin.count())



    val startTime5 = System.nanoTime()
    val fullOuterJoin = largeTable.fullOuterJoin(smallTable)
    println("\nFull outer join - ")
    fullOuterJoin.take(20).foreach(println)
    println(fullOuterJoin.count())
    val endTime5 = System.nanoTime()
    val duration5 = (endTime5 - startTime5) / 1e6 // Execution time in milliseconds
    println(s"Execution time for fullOuterJoin: $duration5 milliseconds")


        val balance = calculateTotalBalanceForUsers(userDataRDD)
    println("\n\nTotal Balance per User- ")
    balance.foreach(println)

    val balanceByAccType = calculateBalanceForUserPerAccType(userDataRDD)
    println("\n\nTotal Balance per Account Type- ")
    balanceByAccType.foreach(println)

    val userWith2SavingAcc = findUsersWithTwoSavingsAccounts(userDataRDD)
    println("\n\nUsers with 2 Savings Account- ")
    userWith2SavingAcc.foreach(println)

    val userByTransaction = findUsersByTransactionDate(userDataRDD, "2024-01-10T10:13:56")
    println("\n\nUser for given TransactionDate - ")
    userByTransaction.foreach(println)
    println("\n\n")


    println(countUsersByBank(userDataRDD))
    println("\n\n")


    val findAccountsForUser = findAccountsForUserId(userId, userDataRDD)
        println(s"Accounts For $userId  -  ")
        findAccountsForUser.foreach { account =>
              println(account)
            }

      val totalByBank = calculateTotalBalanceByBank(userDataRDD)
    println("\nTotal balance as per Bank - ")
    totalByBank.foreach { case (bank, total) =>
      println(f"$bank - ${total}%.2f")
    }


    val totalByAccountType = calculateTotalBalanceByAccountType(userDataRDD)
    println("\nTotal balance as per Account Type - ")
    totalByAccountType.foreach { case (bank, total) =>
      println(f"$bank - ${total}%.2f")
    }

    val missingValues = nullValues(userDataRDD)
    println("\n\nUsers with Null value with count")
    missingValues.take(2).foreach(println)
    println(missingValues.count())

    val lastNamesCount = userDataRDD.flatMap(_.name.lastName)
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(3)
      .map(x => (x._1, x._2))
  println("\n3 Most Common Last Name")
    lastNamesCount.foreach(println)

    val replaceNullValues = missingValues.map {
      user =>

        val randomIndex = Random.nextInt(3)
        val newLastName = lastNamesCount(randomIndex)._1
        user.copy(name = user.name.copy(lastName = Some(newLastName)))
    }
    println("\n\nReplaced Null values - ")
    replaceNullValues.take(10).foreach(println)



    println("\n\nhurray")
    Thread.sleep(10000000)

    sc.stop()
  }

  def nullValues(rdd: RDD[User]) = {
    rdd.filter(_.name.lastName.isEmpty)
  }

  def calculateTotalBalanceForUsers(rdd: RDD[User]) = {
    val data = rdd.map(x => (x.user_id, x.account.balance))
      .reduceByKey(_ + _)
      .mapValues(b => f"$b%.2f")

    // Join total balance with user full name
    val userNamesAndBalance = rdd.map(user => (user.user_id, (user.name.firstName, user.name.lastName)))
      .distinct()
      .join(data)
      .map { case (userId, ((firstName, lastName), balance)) =>
        val fullName = lastName match {
          case Some(value) => s"$firstName $value"
          case None => firstName
        }
        (s"Name - $fullName , UserId - ($userId), Total amount = $balance")
      }

    userNamesAndBalance
  }

  def calculateAverageBalance(rdd: RDD[User]): Double = {
    val totalBalance = rdd.map(user => user.account.balance).sum()
    val totalUsers = rdd.count()
    (totalBalance / totalUsers)
  }

  def calculateTotalBalanceByBank(rdd: RDD[User]) = {
      rdd.map(user => (user.account.bank_name, user.account.balance))
                  .reduceByKey(_+_)
                //      .aggregateByKey(0.0)(_ + _, _ + _)
  }

  def calculateTotalBalanceByAccountType(rdd: RDD[User]) = {
    val totalBalanceByBank = rdd.map(user => (user.account.account_type, user.account.balance))
      .reduceByKey(_ + _)
    totalBalanceByBank
  }

  def calculateTotalBalanceByAccountTypeAndBank(rdd: RDD[User]) = {
    val totalBalanceByAccountTypeAndBank = rdd.map(user => ((user.account.bank_name, user.account.account_type), user.account.balance))
      .aggregateByKey(0.0)(_ + _, _ + _)
    totalBalanceByAccountTypeAndBank
  }

  def calculateBalanceForUserPerAccType(rdd: RDD[User]) = {
    val data = rdd.map { user =>
      ((user.user_id, user.account.account_type), user.account.balance)
    }

    val balance = data.reduceByKey(_ + _)
    balance.sortBy(_._1._1)
  }


  def calculateAverageBalanceByBank(rdd: RDD[User]): RDD[(String, Double)] = {
    val totalBalanceByBank = rdd.map(user => (user.account.bank_name, user.account.balance))
      .aggregateByKey((0.0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
    totalBalanceByBank.mapValues { case (total, count) => total / count }
  }



  def findUsersWithMultipleAccounts(rdd: RDD[User]) = {
    rdd.map(x => (x.user_id, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= 2)
  }

  def findUsersWithTwoSavingsAccounts(rdd: RDD[User]) = {
    val data = rdd.filter(user => user.account.account_type == "savings")
      .map(user => (user.user_id, 1))
      .reduceByKey(_ + _)
    data.filter { case (_, count) => count == 2 }
  }

  def findUsersByTransactionDate(rdd: RDD[User], transactionDate: String): RDD[User] = {
    rdd.filter(_.account.last_transaction == transactionDate)
  }


  def findAccountsForUserId(id: String, rdd: RDD[User]): List[Account] = {
    val userAccounts = rdd.filter(_.user_id == id).map(_.account).collect().toList
    if(userAccounts.isEmpty) List()
    else userAccounts
  }

  def findBanksForUserId(id: String, rdd: RDD[User]): List[String] = {
    val userBankName = rdd.filter(_.user_id == id).map(_.account.bank_name).collect().toList
    if (userBankName.isEmpty) List()
    else userBankName
  }

  def findAccountDetailsForUserId(id: String, rdd: RDD[User]): RDD[(String, String, Double)] = {
    val userAccounts = rdd.filter(_.user_id == id).map(x => ( x.account.bank_name, x.account.account_number, x.account.balance))
    userAccounts
  }

  def countUsersByBank(rdd: RDD[User])= {
    val data = rdd.map(user => (user.account.bank_name, user.user_id))
    val sbiCount = data.filter(x => x._1 == "SBI").count()
    val axisCount = data.filter(x => x._1 == "AXIS").count()
    val hdfcCount = data.filter(x => x._1 == "HDFC").count()
    val result = s"User counts -\n" +
      s"sbi - $sbiCount\n" +
      s"axis - $axisCount\n" +
      s"hdfc - $hdfcCount"

    result
  }

  def countUsersByBankAdvance(rdd: RDD[User])= {
    val data = rdd.map(user => (user.account.bank_name, 1))
    val bankCounts = data.aggregateByKey(0)(_ + _, _ + _)
      .collect()
      .sortBy(_._1)
    bankCounts
  }

  def filterUsersByAccountType(rdd: RDD[User], accountType: String): RDD[User] = {
    rdd.filter(_.account.account_type == accountType)
  }

  def findUsersWithLowestBalance(rdd: RDD[User]): RDD[User] = {
    val minBalance = rdd.map(user => user.account.balance).min
    rdd.filter(_.account.balance == minBalance)
  }
}
