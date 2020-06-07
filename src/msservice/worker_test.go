package msservice

import (
	"log"
	"pbservice"
	"testing"
)

func checkValue(worker *Worker, key string, value string) {
	v := worker.Get(key)
	log.Printf("Get(%v) -> %v, expected %v", key, v, value)
	if v != value {
		log.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
}

func TestFunc(t *testing.T) {
	worker := &Worker{}
	worker.initWorker()
	worker.StartWorker(1)

	for i := 0; i < 100; i++ {
		worker.Put(string(i), string(i*2))
		checkValue(worker, string(i), string(2*i))
	}

	for i := 0; i < 100; i++ {
		worker.Delete(string(i))
	}

	for i := 0; i < 100; i++ {
		checkValue(worker, string(i), pbservice.KeyInexsitence)
	}
}
