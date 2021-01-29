package raft

import (
	"sync"
)

var commands = []interface{}{"A","B","C","D","E","F","G","H","I","J","K","L","M","N","Am","Bm","Cm","Dm","Em","Fm","Gm","Hm","Im","Jm","Km","Lm","Mm","Nm",
	"An","Bn","Cn","Dn","En","Fn","Gn","Hn","In","Jn","Kn","Ln","Mn","Nn",
	"Aq","Bq","Cq","Dq","Eq","Fq","Gq","Hq","Iq","Jq","Kq","Lq","Mq","Nq"}
var idxer int32 = 0
var locker sync.Mutex
func getCommand() interface{}{
	locker.Lock()
	defer locker.Unlock()
	com := commands[idxer]
	idxer++
	return com
}
//
//func TestLab(t *testing.T) {
//	servers := 3
//	//fixme
//	times := 2
//
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")
//
//	cfg.one(getCommand(), servers, true)
//
//	// put leader and one follower in a partition
//	leader1 := cfg.checkOneLeader()
//	cfg.disconnect((leader1 + 2) % servers)
//	cfg.disconnect((leader1 + 3) % servers)
//	cfg.disconnect((leader1 + 4) % servers)
//
//
//	// submit lots of commands that won't commit
//	for i := 0; i < times; i++ {
//		cfg.rafts[leader1].Start(getCommand())
//	}
//
//	time.Sleep(RaftElectionTimeout / 2)
//
//	cfg.disconnect((leader1 + 0) % servers)
//	cfg.disconnect((leader1 + 1) % servers)
//
//	// allow other partition to recover
//	cfg.connect((leader1 + 2) % servers)
//	cfg.connect((leader1 + 3) % servers)
//	cfg.connect((leader1 + 4) % servers)
//
//	// lots of successful commands to new group.
//	for i := 0; i < times; i++ {
//		cfg.one(getCommand(), 3, true)
//	}
//
//	// now another partitioned leader and one follower
//	leader2 := cfg.checkOneLeader()
//	other := (leader1 + 2) % servers
//	if leader2 == other {
//		other = (leader2 + 1) % servers
//	}
//	cfg.disconnect(other)
//
//	// lots more commands that won't commit
//	for i := 0; i < times; i++ {
//		cfg.rafts[leader2].Start(getCommand())
//	}
//
//	time.Sleep(RaftElectionTimeout / 2)
//
//	// bring original leader back to life,
//	for i := 0; i < servers; i++ {
//		cfg.disconnect(i)
//	}
//	cfg.connect((leader1 + 0) % servers)
//	cfg.connect((leader1 + 1) % servers)
//	cfg.connect(other)
//
//	// lots of successful commands to new group.
//	for i := 0; i < times; i++ {
//		cfg.one(getCommand(), 3, true)
//	}
//
//	// now everyone
//	for i := 0; i < servers; i++ {
//		cfg.connect(i)
//	}
//	cfg.one(getCommand(), servers, true)
//
//	cfg.end()
//	idxer=0
//}
