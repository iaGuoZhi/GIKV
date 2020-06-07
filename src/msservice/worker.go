package msservice

import (
	"pbservice"
	"viewservice"
)

type Worker struct {
	vshost  string
	servers [viewservice.ServerNums]*pbservice.PBServer
}

func (worker *Worker) StartWorker() error {

}
