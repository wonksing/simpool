package simpool

import "sync"

type internalJob struct {
	resChan chan *JobResult
	job     Job
}

// Pool struct
type Pool struct {
	noOfWorkers  int
	maxQueueSize int
	wg           *sync.WaitGroup
	jobChan      chan *internalJob
}

// NewPool create pool object
func NewPool(noOfWorkers int, maxQueueSize int) *Pool {
	var wg sync.WaitGroup
	jobChan := make(chan *internalJob, maxQueueSize)
	p := &Pool{
		noOfWorkers:  noOfWorkers,
		maxQueueSize: maxQueueSize,
		wg:           &wg,
		jobChan:      jobChan,
	}
	p.init()
	return p
}

// Init initializes the pool
func (p *Pool) init() {
	if p.jobChan == nil {
		p.jobChan = make(chan *internalJob, p.maxQueueSize)
	}

	p.wg.Add(p.noOfWorkers)
	for i := 0; i < p.noOfWorkers; i++ {
		go p.startWorkers()
	}
}

func (p *Pool) startWorkers() {
	defer p.wg.Done()

	// it is a blocking operation.
	// wait until a job is received.
	// break when the 'jobChan' is closed and empty.
	for e := range p.jobChan {
		if e != nil {
			res := e.job.Execute()
			if e.resChan != nil {
				// send JobResult to 'resChan'
				e.resChan <- res
				close(e.resChan)
			}
		}
	}
}

// Queue a job into the Pool
func (p *Pool) Queue(job Job) {
	j := &internalJob{
		job: job,
	}
	p.jobChan <- j
}

// QueueAndWait a job into the Pool
func (p *Pool) QueueAndWait(job Job) *JobResult {
	j := &internalJob{
		resChan: make(chan *JobResult, 1),
		job:     job,
	}
	p.jobChan <- j
	return <-j.resChan
}

// Close workers
func (p *Pool) Close() {
	close(p.jobChan)
	p.wg.Wait()
}

// Wait for jobs to finish and get ready to receive jobs again
func (p *Pool) Wait() {
	p.Close()
	p.jobChan = nil
	p.init()
}
