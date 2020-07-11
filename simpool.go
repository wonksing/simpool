package simpool

import "sync"

// Pool struct
type Pool struct {
	noOfWorkers  int
	maxQueueSize int
	wg           *sync.WaitGroup
	jobChan      chan Job
}

// NewPool create pool object
func NewPool(noOfWorkers int, maxQueueSize int) *Pool {
	var wg sync.WaitGroup
	jobChan := make(chan Job, maxQueueSize)
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
	p.wg.Add(p.noOfWorkers)
	for i := 0; i < p.noOfWorkers; i++ {
		go p.startWorkers()
	}
}

func (p *Pool) startWorkers() {
	defer p.wg.Done()

	// it is a blocking operation.
	// wait until a job is received.
	// break when the channel is closed and empty.
	for job := range p.jobChan {
		if job != nil {
			job.Execute()
		}
	}
}

// QueueAndWait pushes a job into the Pool and wait for it to finish
func (p *Pool) QueueAndWait(job JobWithResult) *JobResult {
	p.jobChan <- job
	return job.GetExecutedResult()
}

// Queue a job into the Pool
func (p *Pool) Queue(job Job) {
	p.jobChan <- job
}

// Close workers
func (p *Pool) Close() {
	close(p.jobChan)
	p.wg.Wait()
}
