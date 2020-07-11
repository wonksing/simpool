package simpool

// JobResult struct
type JobResult struct {
	Res interface{}
	Err error
}

// Job interface
type Job interface {
	Execute()
}

// JobSimple interface
type JobWithResult interface {
	Job
	GetExecutedResult() *JobResult
}
