package workers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cloudflare/backoff"
	"github.com/saladtechnologies/salad-cloud-imds-sdk-go/pkg/saladcloudimdssdk"
	queues_api "github.com/saladtechnologies/salad-cloud-job-queue-worker/internal/apis/queues"
	"github.com/saladtechnologies/salad-cloud-job-queue-worker/pkg/config"
	"github.com/saladtechnologies/salad-cloud-job-queue-worker/pkg/jobs"
	"github.com/saladtechnologies/salad-cloud-job-queue-worker/pkg/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errWorkloadInstanceNotFound = errors.New("workload instance not found")
var errJobNotFound = errors.New("job not found")
var errOutputInvalid = errors.New("job output invalid")

func Run(ctx context.Context, config config.Config, executor jobs.HTTPJobExecutor, version string) error {
	logger := log.FromContext(ctx)
	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()

	client := newMetadataClient(config.MetadataURI)
	conn, err := newQueueConnection(workerCtx, config.ServiceEndpoint, config.ServiceUseTLS, version)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			logger.Error("failed to close gRPC connection", "error", err)
		}
	}()

	readinessPoller := newReadinessPoller(client)
	go func() {
		err := readinessPoller.poll(workerCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// Expected on stop.
			} else if errors.Is(err, errWorkloadInstanceNotFound) {
				cancelWorker()
			} else {
				panic("an unexpected error occurred")
			}
		}
	}()

	currentJobStore := &atomic.Pointer[queues_api.Job]{}
	jobPoller := newJobPoller(client, conn, currentJobStore)
	var cancelJobPoller context.CancelFunc

	// Main loop.
	ready := false
	for {
		// Assignment phase.
		var handler *jobHandler
	AssignmentPhaseLoop:
		for {
			select {
			case <-workerCtx.Done():
				if cancelJobPoller != nil {
					cancelJobPoller()
				}
				return workerCtx.Err()
			case nextReady := <-readinessPoller.next():
				if nextReady != ready {
					ready = nextReady
					logger.Info("readiness changed", "ready", ready)
					if ready && cancelJobPoller == nil {
						var jobPollerCtx context.Context
						jobPollerCtx, cancelJobPoller = context.WithCancel(workerCtx)
						go func() {
							err := jobPoller.poll(jobPollerCtx)
							if err != nil {
								if errors.Is(err, context.Canceled) {
									// Expected on stop.
								} else if errors.Is(err, errWorkloadInstanceNotFound) {
									cancelWorker()
								} else if errors.Is(err, errJobNotFound) {
									// TODO: Handle job not found.
									logger.Warn("job not found")
								} else {
									panic("an unexpected error occurred")
								}
							}
						}()
					} else if !ready && cancelJobPoller != nil {
						cancelJobPoller()
						cancelJobPoller = nil
					}
				}
			case nextJob := <-jobPoller.next():
				currentJobStore.Store(nextJob)
				var requestURL string
				if strings.HasPrefix(nextJob.Path, "/") {
					requestURL = fmt.Sprintf("http://localhost:%d%s", nextJob.Port, nextJob.Path)
				} else {
					requestURL = fmt.Sprintf("http://localhost:%d/%s", nextJob.Port, nextJob.Path)
				}
				handler = newJobHandler(client, conn, jobs.HTTPJob{
					JobId:         nextJob.JobId,
					RequestMethod: http.MethodPost,
					RequestURL:    requestURL,
					RequestBody:   nextJob.Input,
				}, executor)
				go func() {
					err := handler.execute(workerCtx)
					if err != nil {
						if errors.Is(err, context.Canceled) {
							// Expected on stop.
						} else {
							panic("an unexpected error occurred")
						}
					}
				}()
				break AssignmentPhaseLoop
			}
		}

		// Execute phase.
	ExecutePhaseLoop:
		for {
			select {
			case <-workerCtx.Done():
				if cancelJobPoller != nil {
					cancelJobPoller()
				}
				return workerCtx.Err()
			case nextReady := <-readinessPoller.next():
				if nextReady != ready {
					ready = nextReady
					logger.Info("readiness changed", "ready", ready)
					if ready && cancelJobPoller == nil {
						var jobPollerCtx context.Context
						jobPollerCtx, cancelJobPoller = context.WithCancel(workerCtx)
						go func() {
							err := jobPoller.poll(jobPollerCtx)
							if err != nil {
								if errors.Is(err, context.Canceled) {
									// Expected on stop.
								} else if errors.Is(err, errWorkloadInstanceNotFound) {
									cancelWorker()
								} else if errors.Is(err, errJobNotFound) {
									// TODO: Handle job not found.
									logger.Warn("job not found")
								} else {
									panic("an unexpected error occurred")
								}
							}
						}()
					} else if !ready && cancelJobPoller != nil {
						cancelJobPoller()
						cancelJobPoller = nil
					}
				}
			case <-handler.done():
				currentJobStore.Store(nil)
				break ExecutePhaseLoop
			}
		}
	}
}

// Represents a long-running goroutine that polls for readiness changes.
type readinessPoller struct {
	client    *saladcloudimdssdk.SaladCloudImdsSdk
	readiness chan bool
}

// Creates a new readiness poller.
func newReadinessPoller(client *saladcloudimdssdk.SaladCloudImdsSdk) *readinessPoller {
	return &readinessPoller{
		client:    client,
		readiness: make(chan bool),
	}
}

// Gets a channel that will receive readiness changes.
//
// This channel is unbuffered and the readiness poller will block when no reader
// is available. This will delay the next attempt to poll for readiness.
func (p *readinessPoller) next() <-chan bool {
	return p.readiness
}

// Polls for readiness changes.
//
// This function will block until the context is canceled or an error occurs.
func (p *readinessPoller) poll(ctx context.Context) error {
	logger := log.FromContext(ctx)
	failures := 0
	for {
		// The client will automatically retry several times on network errors,
		// invalid responses, 429's, and 5xx's (except 501's) using a short,
		// exponential backoff algorithm. Eventually, it will give up and return
		// the last error.
		resp, err := p.client.Metadata.GetStatus(ctx)
		retryDelay := 2 * time.Minute // Default retry delay for errors.
		if err != nil {
			failures++
			logger.Error("failed to fetch workload instance status", "error", err)
		}

		if resp != nil {
			switch resp.Metadata.StatusCode {
			case http.StatusOK:
				if resp.Data.GetReady() != nil {
					failures = 0
					ready := *resp.Data.GetReady()
					logger.Debug("fetched workload instance status", "status", ready)
					retryDelay = 5 * time.Second // Retry delay for successes.
					select {
					case <-ctx.Done():
						return ctx.Err()
					case p.readiness <- ready:
					}
				} else {
					failures++
					logger.Error("failed to receive JSON response body fetching workload instance status")
				}
			case http.StatusNotFound:
				logger.Error("workload instance does not exist")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case p.readiness <- false:
				}
				return errWorkloadInstanceNotFound
			default:
				failures++
				logger.Error("failed to fetch workload instance status", "status", resp.Metadata.StatusCode)
			}
		}

		if failures >= 3 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.readiness <- false:
			}
		}

		delay := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			delay.Stop()
			return ctx.Err()
		case <-delay.C:
		}
	}
}

// Represents a long-running goroutine that polls for jobs.
type jobPoller struct {
	client     *saladcloudimdssdk.SaladCloudImdsSdk
	conn       *grpc.ClientConn
	currentJob *atomic.Pointer[queues_api.Job]
	jobs       chan *queues_api.Job
}

// Creates a new job poller.
func newJobPoller(client *saladcloudimdssdk.SaladCloudImdsSdk, conn *grpc.ClientConn, currentJob *atomic.Pointer[queues_api.Job]) *jobPoller {
	return &jobPoller{
		client:     client,
		conn:       conn,
		currentJob: currentJob,
		jobs:       make(chan *queues_api.Job),
	}
}

// Gets a channel that will receive jobs.
//
// This channel is unbuffered and the job poller will block when no reader is
// available. This will delay the next attempt to poll for jobs.
func (p *jobPoller) next() <-chan *queues_api.Job {
	return p.jobs
}

// Polls for jobs.
//
// This function will block until the context is canceled or an error occurs.
func (p *jobPoller) poll(ctx context.Context) error {
	logger := log.FromContext(ctx)
	bo := backoff.New(2*time.Minute, 1*time.Second)
	for {
		// The client will automatically retry several times on network errors,
		// invalid responses, 429's, and 5xx's (except 501's) using a short,
		// exponential backoff algorithm. Eventually, it will give up and return
		// the last error.
		resp, err := p.client.Metadata.GetToken(ctx)
		if err != nil {
			logger.Error("failed to fetch workload instance token", "error", err)
		}

		var token string
		if resp != nil {
			switch resp.Metadata.StatusCode {
			case http.StatusOK:
				if resp.Data.GetJwt() != nil {
					token = *resp.Data.GetJwt()
					logger.Debug("fetched workload instance token", "token", token)
				} else {
					logger.Error("failed to receive JSON response body fetching workload instance token")
				}
			case http.StatusNotFound:
				return errWorkloadInstanceNotFound
			default:
				logger.Error("failed to fetch workload instance token", "status", resp.Metadata.StatusCode)
			}
		}

		if token == "" {
			delay := time.NewTimer(2 * time.Minute)
			select {
			case <-ctx.Done():
				delay.Stop()
				return ctx.Err()
			case <-delay.C:
				continue
			}
		}

		// TODO: Create a new cancelable context with "cause"; start the watchdog timer goroutine with this context.
		authCtx := createAuthorizedContext(ctx, token)
		client := queues_api.NewJobQueueWorkerServiceClient(p.conn)
		for {
			currentJob := p.currentJob.Load()
			currentJobId := ""
			if currentJob != nil {
				currentJobId = currentJob.JobId
			}

			// The client will automatically retry several times on network
			// errors and invalid responses using a short, exponential backoff
			// algorithm. Eventually, it will give up and return the last error.
			stream, err := client.AcceptJobs(authCtx, &queues_api.AcceptJobsRequest{
				CurrentJobId: currentJobId,
			})
			stat, ok := status.FromError(err)
			if !ok {
				logger.Error("failed to open job stream", "error", err)
				delay := time.NewTimer(2 * time.Minute)
				select {
				case <-ctx.Done():
					delay.Stop()
					return ctx.Err()
				case <-delay.C:
					continue
				}
			}

			reauth := false
			switch stat.Code() {
			case codes.Canceled:
				return ctx.Err()
			case codes.NotFound:
				return errJobNotFound
			case codes.OK:
			ReceiveLoop:
				for {
					resp, err := stream.Recv()
					if err == nil {
						switch msg := resp.Message.(type) {
						case *queues_api.AcceptJobsResponse_Heartbeat:
							// TODO: Reset the watchdog timer.
							logger.Debug("received heartbeat")
						case *queues_api.AcceptJobsResponse_Job:
							logger.Info("received job", "job", msg.Job)
							select {
							case <-ctx.Done():
								return ctx.Err()
							case p.jobs <- msg.Job:
								continue
							}
						}
					} else if err == io.EOF {
						logger.Info("job stream closed")
						break ReceiveLoop
					} else {
						stat, ok := status.FromError(err)
						if !ok {
							logger.Error("failed to receive job", "error", err)
							break ReceiveLoop
						}

						switch stat.Code() {
						case codes.Canceled:
							return ctx.Err()
						case codes.NotFound:
							return errJobNotFound
						case codes.PermissionDenied:
							reauth = true
							break ReceiveLoop
						case codes.Unauthenticated:
							reauth = true
							break ReceiveLoop
						default:
							logger.Error("failed to receive job", "error", err)
							break ReceiveLoop
						}
					}
				}
			case codes.PermissionDenied:
				reauth = true
			case codes.Unauthenticated:
				reauth = true
			default:
				logger.Error("failed to open job stream", "error", err)
				delay := time.NewTimer(2 * time.Minute)
				select {
				case <-ctx.Done():
					delay.Stop()
					return ctx.Err()
				case <-delay.C:
					continue
				}
			}

			if reauth {
				logger.Info("token expired", "token", token)
				break
			} else {
				bo.Reset()
			}
		}

		delay := time.NewTimer(bo.Duration())
		select {
		case <-ctx.Done():
			delay.Stop()
			return ctx.Err()
		case <-delay.C:
			continue
		}
	}
}

// Represents a long-running goroutine that executes a job and uploads the result.
type jobHandler struct {
	client   *saladcloudimdssdk.SaladCloudImdsSdk
	conn     *grpc.ClientConn
	d        chan struct{}
	executor jobs.HTTPJobExecutor
	job      jobs.HTTPJob
}

// Creates a new job handler.
func newJobHandler(client *saladcloudimdssdk.SaladCloudImdsSdk, conn *grpc.ClientConn, job jobs.HTTPJob, executor jobs.HTTPJobExecutor) *jobHandler {
	return &jobHandler{
		client:   client,
		conn:     conn,
		d:        make(chan struct{}),
		executor: executor,
		job:      job,
	}
}

// Gets a channel that will receive a signal when the job handler is done.
//
// This channel is unbuffered and the job handler will block when no reader is
// available.
func (h *jobHandler) done() <-chan struct{} {
	return h.d
}

// Executes the job and uploads the result.
func (h *jobHandler) execute(ctx context.Context) error {
	logger := log.FromContext(ctx)
	responseBody, err := h.executor(ctx, h.job)
	if err != nil {
		logger.Warn("failed to execute job", "error", err)
		err = h.reject(ctx)
	} else {
		err = h.complete(ctx, responseBody)
		if errors.Is(err, errOutputInvalid) {
			logger.Warn("invalid job output", "jobId", h.job.JobId)
			err = h.reject(ctx)
		}
	}

	h.d <- struct{}{}
	return err
}

func (h *jobHandler) complete(ctx context.Context, responseBody []byte) error {
	logger := log.FromContext(ctx)
	bo := backoff.New(time.Minute*2, time.Second*1)
	for {
		// The client will automatically retry several times on network errors,
		// invalid responses, 429's, and 5xx's (except 501's) using a short,
		// exponential backoff algorithm. Eventually, it will give up and return
		// the last error.
		resp, err := h.client.Metadata.GetToken(ctx)
		if err != nil {
			logger.Error("failed to fetch workload instance token", "error", err)
		}

		var token string
		if resp != nil {
			switch resp.Metadata.StatusCode {
			case http.StatusOK:
				if resp.Data.GetJwt() != nil {
					token = *resp.Data.GetJwt()
					logger.Debug("fetched workload instance token", "token", token)
				} else {
					logger.Error("failed to receive JSON response body fetching workload instance token")
				}
			case http.StatusNotFound:
				return errWorkloadInstanceNotFound
			default:
				logger.Error("failed to fetch workload instance token", "status", resp.Metadata.StatusCode)
			}
		}

		if token == "" {
			delay := time.NewTimer(2 * time.Minute)
			select {
			case <-ctx.Done():
				delay.Stop()
				return ctx.Err()
			case <-delay.C:
				continue
			}
		}

		authCtx := createAuthorizedContext(ctx, token)
		client := queues_api.NewJobQueueWorkerServiceClient(h.conn)
		for {
			// The client will automatically retry several times on network
			// errors and invalid responses using a short, exponential backoff
			// algorithm. Eventually, it will give up and return the last error.
			_, err := client.CompleteJob(authCtx, &queues_api.CompleteJobRequest{
				JobId:  h.job.JobId,
				Output: responseBody,
			})
			stat, ok := status.FromError(err)
			if !ok {
				logger.Error("failed to complete job", "error", err)
				delay := time.NewTimer(2 * time.Minute)
				select {
				case <-ctx.Done():
					delay.Stop()
					return ctx.Err()
				case <-delay.C:
					continue
				}
			}

			reauth := false
			switch stat.Code() {
			case codes.Canceled:
				return ctx.Err()
			case codes.InvalidArgument:
				return errOutputInvalid
			case codes.NotFound:
				logger.Info("job not found", "jobId", h.job.JobId)
				return nil
			case codes.OK:
				logger.Info("job completed", "jobId", h.job.JobId)
				return nil
			case codes.PermissionDenied:
				reauth = true
			case codes.ResourceExhausted:
				return errOutputInvalid
			case codes.Unauthenticated:
				reauth = true
			default:
				logger.Error("failed to complete job", "error", err)
				delay := time.NewTimer(2 * time.Minute)
				select {
				case <-ctx.Done():
					delay.Stop()
					return ctx.Err()
				case <-delay.C:
					continue
				}
			}

			if reauth {
				logger.Info("token expired", "token", token)
				break
			} else {
				bo.Reset()
			}
		}

		delay := time.NewTimer(bo.Duration())
		select {
		case <-ctx.Done():
			delay.Stop()
			return ctx.Err()
		case <-delay.C:
			continue
		}
	}
}

func (h *jobHandler) reject(ctx context.Context) error {
	logger := log.FromContext(ctx)
	bo := backoff.New(time.Minute*2, time.Second*1)
	for {
		// The client will automatically retry several times on network errors,
		// invalid responses, 429's, and 5xx's (except 501's) using a short,
		// exponential backoff algorithm. Eventually, it will give up and return
		// the last error.
		resp, err := h.client.Metadata.GetToken(ctx)
		if err != nil {
			logger.Error("failed to fetch workload instance token", "error", err)
		}

		var token string
		if resp != nil {
			switch resp.Metadata.StatusCode {
			case http.StatusOK:
				if resp.Data.GetJwt() != nil {
					token = *resp.Data.GetJwt()
					logger.Debug("fetched workload instance token", "token", token)
				} else {
					logger.Error("failed to receive JSON response body fetching workload instance token")
				}
			case http.StatusNotFound:
				return errWorkloadInstanceNotFound
			default:
				logger.Error("failed to fetch workload instance token", "status", resp.Metadata.StatusCode)
			}
		}

		if token == "" {
			delay := time.NewTimer(2 * time.Minute)
			select {
			case <-ctx.Done():
				delay.Stop()
				return ctx.Err()
			case <-delay.C:
				continue
			}
		}

		authCtx := createAuthorizedContext(ctx, token)
		client := queues_api.NewJobQueueWorkerServiceClient(h.conn)
		for {
			// The client will automatically retry several times on network
			// errors and invalid responses using a short, exponential backoff
			// algorithm. Eventually, it will give up and return the last error.
			_, err := client.RejectJob(authCtx, &queues_api.RejectJobRequest{
				JobId: h.job.JobId,
			})
			stat, ok := status.FromError(err)
			if !ok {
				logger.Error("failed to reject job", "error", err)
				delay := time.NewTimer(2 * time.Minute)
				select {
				case <-ctx.Done():
					delay.Stop()
					return ctx.Err()
				case <-delay.C:
					continue
				}
			}

			reauth := false
			switch stat.Code() {
			case codes.Canceled:
				return ctx.Err()
			case codes.NotFound:
				logger.Info("job not found", "jobId", h.job.JobId)
				return nil
			case codes.OK:
				logger.Info("job rejected", "jobId", h.job.JobId)
				return nil
			case codes.PermissionDenied:
				reauth = true
			case codes.Unauthenticated:
				reauth = true
			default:
				logger.Error("failed to reject job", "error", err)
				delay := time.NewTimer(2 * time.Minute)
				select {
				case <-ctx.Done():
					delay.Stop()
					return ctx.Err()
				case <-delay.C:
					continue
				}
			}

			if reauth {
				logger.Info("token expired", "token", token)
				break
			} else {
				bo.Reset()
			}
		}

		delay := time.NewTimer(bo.Duration())
		select {
		case <-ctx.Done():
			delay.Stop()
			return ctx.Err()
		case <-delay.C:
			continue
		}
	}
}
