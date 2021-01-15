package kvraft

import (
	"log"
	"strconv"
)

var DebugLab = []bool{true,true}
var DebugLogger = 0

func (ck *Clerk) DPrintf(labNo int,str string,a ...interface{}){
	if DebugLab[labNo-1] {
		str = "#Client."+strconv.Itoa(ck.clientId)+":" + str
		if DebugLogger > 0 {
			log.Printf(str, a...)
		}
		return
	}
}


func (kv *KVServer) DPrintf(labNo int,str string,a ...interface{}){
	if DebugLab[labNo-1] {
		str = "#Server."+strconv.Itoa(kv.me)+":" + str
		if DebugLogger > 0 {
			log.Printf(str, a...)
		}
		return
	}
}

