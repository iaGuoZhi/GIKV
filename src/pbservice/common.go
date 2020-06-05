package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	KeyInexsitence = "___KEY__INEXISTENCE"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type DeleteArgs struct {
	Key string
}

type DeleteReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type MoveDBArgs struct {
	DB map[string]string
}

type MoveDBReply struct {
	Err Err
}
