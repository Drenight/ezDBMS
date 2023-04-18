s1 = '1==1'
s2 = '1==0'

s = str(eval(s2))+" or "+str(eval(s1))

if(eval(s)):
    print(7)
else:
    print(6)
