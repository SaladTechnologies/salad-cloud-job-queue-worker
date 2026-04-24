package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/saladtechnologies/salad-cloud-job-queue-worker/test/wiremock/containers"
)

func main() {
	defaultLogger := slog.Default()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	defaultLogger.Info("starting WireMock...")
	wiremock, err := containers.StartWiremockContainer(context.Background(), []containers.WiremockMapping{
		{
			Name:    "reallocate.json",
			Content: "{\"request\":{\"method\":\"POST\",\"urlPath\":\"/v1/reallocate\"},\"response\":{\"status\":204}}",
		},
		{
			Name:    "status.json",
			Content: "{\"request\":{\"method\":\"GET\",\"urlPath\":\"/v1/status\"},\"response\":{\"status\":200,\"headers\":{\"Content-Type\":\"application/json\"},\"jsonBody\":{\"ready\":true,\"started\":true}}}",
		},
		{
			Name:    "token.json",
			Content: "{\"request\":{\"method\":\"GET\",\"urlPath\":\"/v1/token\"},\"response\":{\"status\":200,\"headers\":{\"Content-Type\":\"application/json\"},\"jsonBody\":{\"jwt\":\"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzYWxhZF9tYWNoaW5lX2lkIjoiMTIzIn0.sDcpi60mMSxwN5ZQcX8oyhgjAjdQBXjubLhOkjNfXKE\"}}}",
		},
	})
	if err != nil {
		defaultLogger.Error("failed to start WireMock", "err", err)
		os.Exit(1)
	}

	defaultLogger.Info("WireMock started", "URL", wiremock.BaseURL())
	<-signals
	defaultLogger.Info("stopping WireMock...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = wiremock.Stop(ctx)
	if err != nil {
		defaultLogger.Error("failed to stop WireMock", "err", err)
		os.Exit(1)
	}

	defaultLogger.Info("stopped WireMock")
	os.Exit(0)
}
