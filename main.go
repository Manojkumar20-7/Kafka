package main

import "flag"

func main(){
	option:=flag.Int("i",0,"")
	flag.Parse()
	switch *option{
	case 0:
		producer()
	case 1:
		consumer()
	}
}