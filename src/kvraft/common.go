package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {

	ClientId int
	Seq int

	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	IsLeader bool
	Err      Err
}

type GetArgs struct {

	ClientId int
	Seq int

	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	IsLeader bool
	Err      Err
	Value    string
}
