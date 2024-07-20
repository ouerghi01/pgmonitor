#!/usr/bin/python


def getStatus(V , Wn , Cr=0 , Mn=0 , Mx=0) :
	Cr = int(Cr)
	Wn = int(Wn)
	V = int(V)

	if Cr != 0 :
		if V < Wn and Wn < Cr  : 
			return 0
		elif Wn <= V and V < Cr : 
			return 1
		elif Wn <= Cr and Cr <= V  :
			return 2
		else :
			return 3
	elif Cr == 0 :
                if V < Wn  :
                        return 0
                elif V >= Wn  :
                        return 1
                else :
                        return 3
