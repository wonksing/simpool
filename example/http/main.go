package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/wonksing/simpool"
)

var (
	gp           *simpool.Pool
	addr         string
	numWorkers   int
	maxQueueSize int
)

func init() {
	flag.StringVar(&addr, "addr", ":8888", "listening address")
	flag.IntVar(&numWorkers, "w", 16, "number of workers")
	flag.IntVar(&maxQueueSize, "q", 320, "max queue size")
	flag.Parse()
}
func longJobHnadler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatalln(err)
	}
	job := NewLongJob(body)
	jr := gp.QueueAndWait(job)
	if jr.Err != nil {
		log.Println(jr.Err)
		return
	}
	w.Write([]byte(jr.Res.(string)))
}

func main() {
	gp = simpool.NewPool(numWorkers, maxQueueSize)

	router := http.NewServeMux()
	router.HandleFunc("/longjob", longJobHnadler)

	server := &http.Server{
		Addr:         addr,
		WriteTimeout: time.Duration(30) * time.Second,
		ReadTimeout:  time.Duration(30) * time.Second,
		Handler:      router,
	}

	err := server.ListenAndServe()
	if err != nil {
		log.Println(err)
	}
	gp.Close()

}

type LongJob struct {
	body []byte
	res  chan *simpool.JobResult
}

func NewLongJob(body []byte) *LongJob {
	return &LongJob{
		body: body,
		res:  make(chan *simpool.JobResult),
	}
}
func (s *LongJob) Execute() {
	defer close(s.res)

	log.Println("Executing Long Job")

	bodyStr := string(s.body)
	// rn := rand.Intn(100)
	time.Sleep(time.Second * time.Duration(6))
	jr := &simpool.JobResult{
		Res: bodyStr,
		Err: nil,
	}
	log.Println("Finished Long Job")
	s.res <- jr
}
func (s *LongJob) GetExecutedResult() *simpool.JobResult {
	return <-s.res
}
