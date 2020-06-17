package viewservice

import (
	"net"
	"sync"
	"time"
)

// ViewServer ... ViewServer provide a simple service for primary and backup in worker node
// each worker node is consist of a viewserver ,a primary and two backup
// every time interval, both primary and backup should ping viewserver, and
// viewserver will record last ping time for each primary and backup. after
// specific time without receiving from  a primary or backup. viewserver will
// judge its death. the next node behind it will be promoted
type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	view     View    // current view
	newView  View    // next view
	update   bool    // can view be updated?
	promote  [2]bool // can backup 1&2 be promoted
	lastPing map[string]time.Time
}

// BackupNums ... in my implemention, two backup for every primary
const BackupNums = 2

// ServerNums ... total server number is 3
const ServerNums = 3

// View ... each View contains a primary and at most two backup
type View struct {
	Viewnum uint
	Primary string
	Backup  [BackupNums]string
}

// PingInterval ... client should send ping to view server every PingInterval
const PingInterval = time.Millisecond * 100

// DeadPings ... viewserver will notify a node's dead after
// it miss Ping for sequential DeadPings time
const DeadPings = 5

// PingArgs ...
type PingArgs struct {
	Me      string //  rpc address
	Viewnum uint   // caller's notion of current view #
}

// PingReply ...
type PingReply struct {
	View View
}

// GetArgs ... fetch current View for testing
type GetArgs struct {
}

// GetReply ... for testing
type GetReply struct {
	View View
}

// Err ... Simpe Error
type Err struct {
	info string
}

func (e *Err) Error() string {
	return e.info + "\n"
}
