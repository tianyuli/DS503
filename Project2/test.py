import io

f= open("output/part-r-00000","r")
lines = f.readlines()
rec = []
for l in lines:
	rect = l.split("(")[0]
	rec = rect.split(",")
	point = l.split("(")[1].split(")")[0]
	p = point.split(",")

	if(int(p[0])<int(rec[0]) or int(p[0])>(int(rec[0])+int(rec[3]))):
		print("-------------")
		print(rec)
		print(p)
		print("Fail")

	if(int(p[1])<int(rec[1]) or int(p[1])>(int(rec[1])+int(rec[2]))):
		print("-------------")
		print(rec)
		print(p)
		print("Fail")
print("Success")
