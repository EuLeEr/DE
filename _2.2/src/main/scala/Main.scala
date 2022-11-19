import java.lang.Math.round

object Main {
  def main(args: Array[String]): Unit = {
    var arrl = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println("Original Array: " + arrl.mkString(" "))
    arrl.foreach(println)

    println("Hello world!")
    /* a.    Напишите программу, которая:
       i.     выводит фразу «Hello, Scala!» справа налево
     */
    println("Hello, Scala!".reverse)
    //ii.    переводит всю фразу в верхний регистр
    println("Hello, Scala!".toUpperCase())
    //iii.   удаляет символ «!»
    println("Hello, Scala!".dropRight(1))
    // iV. добавляет в конец фразы «and goodbye python!»
    println("Hello, Scala!".concat(" and goodbye python!"))
    /* Напишите программу, которая вычисляет ежемесячный оклад сотрудника после вычета налогов.
    На вход вашей программе подается значение годового дохода до вычета налогов,
    размер премии – в процентах от годового дохода и компенсация питания.
     */
    var salary = 100000
    var tax = 0.13
    var monthSalary = salary * (1 - tax) / 12
    /* с.     Напишите программу, которая рассчитывает для каждого сотрудника отклонение(в процентах)
    от среднего значения оклада на уровень всего отдела.
    В итоговом значении должно учитываться в большую или меньшую сторону отклоняется размер оклада.
    На вход вышей программе подаются все значения, аналогичные предыдущей программе,
    а также список со значениями окладов сотрудников отдела 100, 150, 200, 80, 120, 75.
     */
    var salaryList = Array(100, 150, 200, 80, 120, 75)
    var averageSalary = salaryList.sum / salaryList.length
    //  var deviation = (salaryList.map(x => (x - averageSalary) / averageSalary * 100)).mkString(" ")
    //  println("Deviation: " + deviation)
    var deviation = salaryList.map(x => (x - averageSalary)) /// averageSalary ) //* 100)
    deviation.foreach(println)
    /*d.   Попробуйте рассчитать новую зарплату сотрудника, добавив(или отняв, если сотрудник плохо себя вел)
    необходимую сумму с учетом результатов прошлого задания. Совершенно не понял , о чем речь.
    Поэтому просто добавил по рублю
    Добавьте его зарплату в список и вычислите значение самой высокой зарплаты и самой низкой.
     */
    var newSalary = salaryList.map(x => (x + 1))
    newSalary.foreach(println)
    var maxSalary = newSalary.max
    var minSalary = newSalary.min
    println("Max salary: " + maxSalary)
    println("Min salary: " + minSalary)

    /* e.     Также в вашу команду пришли два специалиста с окладами 350 и 90 тысяч рублей.
    Попробуйте отсортировать список сотрудников по уровню оклада от меньшего к большему.
     */
    var newSalaryList = Array(350, 90)
    var allSalaryList = newSalaryList ++ newSalary
    allSalaryList.sorted.foreach(println)
    /* f.     Кажется, вы взяли в вашу команду еще одного сотрудника и предложили ему оклад 130 тысяч.
      Вычислите самостоятельно номер сотрудника в списке так, чтобы сортировка не нарушилась и
      добавьте его на это место.
     */
    println("Task f")
    var newSalaryList2 = Array(130)
    var allSalaryList2 = newSalaryList2 ++ allSalaryList
    allSalaryList2.sorted.foreach(println)
    /* g.       Попробуйте вывести номера сотрудников из полученного списка, которые попадают под категорию middle.
        На входе программе подается «вилка» зарплаты специалистов уровня middle.
     */
    println("Task g")
    var middleSalary = Array(100, 200)
    var middleSalaryList = allSalaryList2.filter(x => (x >= middleSalary(0) && x <= middleSalary(1)))
    middleSalaryList.foreach(println)
    /*  h. Однако наступил кризис и ваши сотрудники требуют повысить зарплату.
          Вам необходимо проиндексировать зарплату каждого сотрудника на уровень инфляции – 7%
     */
    println("Task h")
    var inflation = 0.07
    var newSalaryList3 = allSalaryList2.map(x => (x * (1 + inflation)).round)
    newSalaryList3.foreach(println)
    /*  i. Ваши сотрудники остались недовольны и просят индексацию на уровень рынка. Попробуйте повторить
    ту же операцию,  как и в предыдущем задании, но теперь вам нужно проиндексировать зарплаты
    на процент отклонения от среднего по рынку с учетом уровня специалиста.
    На вход вашей программе подается 3 значения – среднее значение зарплаты на рынке для каждого уровня
    специалистов(junior, middle и senior)
     */
    println("Task i")
    var marketSalary = Array(100, 200, 300)

    var juniorSalary = newSalaryList3.filter(x => (x < marketSalary(0)))
    var middleSalary2 = newSalaryList3.filter(x => (x >= marketSalary(0) && x <= marketSalary(1)))
    var seniorSalary = newSalaryList3.filter(x => (x > marketSalary(1)))
    var juniorSalary2 = juniorSalary.map(x => (x * (1 + (marketSalary(0) - x) / marketSalary(0))))
    var middleSalary3 = middleSalary2.map(x => (x * (1 + (marketSalary(1) - x) / marketSalary(1))))
    var seniorSalary2 = seniorSalary.map(x => (x * (1 + (marketSalary(2) - x) / marketSalary(2))))
    var newSalaryList4 = juniorSalary2 ++ middleSalary3 ++ seniorSalary2
    newSalaryList4.foreach(println)
  }
}