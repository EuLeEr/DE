def unmatched(s):
    chars = list(s)
    opened = []

    
    for i, c in enumerate(chars):
        if c == '{':
            opened.append(i)

        if c == '}':
            if  len(opened) > 0 :
                opened.pop() 
            else:    
                opened.append(i) 
                break
    if len(opened) == 0:
        for i, c in enumerate(chars):
            if c == '(':
                opened.append(i)

            if c == ')':
                if  len(opened) > 0:
                    opened.pop()    
                else:
                    opened.append(i)
                    break

    if len(opened) == 0:
        for i, c in enumerate(chars):
            if c == '[':
                opened.append(i)

            if c == ']':
                if  len(opened) > 0:
                    opened.pop()
                else:
                    opened.append(i)
                    break

    return True if (len(opened) == 0) else False

s = input("Введите набор скобок: ")
print(unmatched(s))