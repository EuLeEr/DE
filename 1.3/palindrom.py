import re
source="rotato"
goal = "rot ator"
tem = re.sub(' ' ,'',goal)
def reverse(s):
    a=""
    i=len(s)-1
    while(i>=0):
        a=a+s[i]
        i-=1
    return a
tem=reverse(tem)    
if tem == source:
    print(True)
else:
    print(False)

