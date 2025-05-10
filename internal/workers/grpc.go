package workers

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/saladtechnologies/salad-cloud-job-queue-worker/pkg/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Creates a connection to the SaladCloud Job Queues service.
func newQueueConnection(ctx context.Context, addr string, useTLS bool, version string) (*grpc.ClientConn, error) {
	var userAgent = fmt.Sprintf("salad-cloud-job-queue-worker/%s grpc-go/%s", version, grpc.Version)
	opts := []grpc.DialOption{grpc.WithUserAgent(userAgent)}
	if useTLS {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: false,
		})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	var retryPolicy = `{
		"methodConfig": [
			{
				"name": [{"service": "salad.grpc.saladcloud_job_queue_worker.v1alpha.JobQueueWorkerService"}],
				"retryPolicy": {
					"maxAttempts": 5,
					"initialBackoff": "0.1s",
					"maxBackoff": "1s",
					"backoffMultiplier": 2,
					"retryableStatusCodes": ["UNKNOWN", "INTERNAL", "UNAVAILABLE"]
				},
				"timeout": "30s",
				"waitForReady": true
			},
			{
				"name": [{"service": "salad.grpc.saladcloud_job_queue_worker.v1alpha.JobQueueWorkerService", "method": "AcceptJobs"}],
				"retryPolicy": {
					"maxAttempts": 5,
					"initialBackoff": "0.1s",
					"maxBackoff": "1s",
					"backoffMultiplier": 2,
					"retryableStatusCodes": ["UNKNOWN", "INTERNAL", "UNAVAILABLE"]
				},
				"timeout": null,
				"waitForReady": true
			}
		]
	}`
	opts = append(opts, grpc.WithDefaultServiceConfig(retryPolicy), grpc.WithDisableServiceConfig())
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		// This call simply validates the configuration. It should not fail.
		return nil, err
	}

	go monitorConnection(ctx, conn)
	return conn, nil
}

// Creates a new context with an authorization token for gRPC calls to the SaladCloud Job Queues service.
func createAuthorizedContext(ctx context.Context, token string) context.Context {
	md := metadata.Pairs("Authorization", fmt.Sprintf("Bearer %s", token))
	return metadata.NewOutgoingContext(ctx, md)
}

// Monitors the state of a gRPC connection and logs changes.
func monitorConnection(ctx context.Context, conn *grpc.ClientConn) {
	logger := log.FromContext(ctx)
	lastState := connectivity.Idle
	for {
		if !conn.WaitForStateChange(ctx, lastState) {
			return
		}

		currentState := conn.GetState()
		if currentState != lastState {
			logger.Info("gRPC connection state changed", "state", currentState)
			lastState = currentState
		}
	}
}
