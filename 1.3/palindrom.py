import re
source=input("Введите строку для проверки на палиндром:")
source = re.sub(' ' ,'',source)
def reverse(s):
    a=""
    i=len(s)-1
    while(i>=0):
        a=a+s[i]
        i-=1
    return a
tem=reverse(source)    
if tem == source:
    print(True)
else:
    print(False)

