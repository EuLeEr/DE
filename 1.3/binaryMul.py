
firstnumber = input("Введите первое число в бинарном формате ") #110
secondnumber = input('Введите второе число в бинарном формате ') #10

 
Multiplication = int(firstnumber, 2) * int(secondnumber, 2)
binaryMul = str(bin(Multiplication))
 
print("\nУмножение = " + binaryMul[2:])
