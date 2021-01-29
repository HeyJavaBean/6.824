package raft

import (
	"log"
	"strconv"
)

var DebugLab = []bool{false,false,false}
var DebugLogger = 0

func (rf *Raft) DPrintf(labNo int,str string,a ...interface{}){
	if DebugLab[labNo-1] {
		str = "#" + strconv.Itoa(rf.me) + ": " + str
		if DebugLogger > 0 {
			log.Printf(str, a...)
		}
		return
	}
}

