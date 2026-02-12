# SaladCloud Job Queue Worker

[![License](https://img.shields.io/github/license/SaladTechnologies/salad-cloud-job-queue-worker)](./LICENSE)

<center>
  <a href="https://github.com/SaladTechnologies/salad-cloud-job-queue-worker"><img alt="SaladCloud Job Queues" src="./images/saladcloud-job-queues-banner.png" width="100%" /></a>
</center>

This project contains the SaladCloud Job Queue Worker, SDK, and samples. Refer to the [Job Queues documentation](https://docs.salad.com/products/sce/job-queues/job-queues) for more information on using this with your SaladCloud-deployed workloads.

> [!NOTE]
> The SaladCloud Job Queue Worker will currently only run successfully on a SaladCloud node due to a dependency on the [SaladCloud Instance Metadata Service (IMDS)](https://docs.salad.com/products/sce/container-groups/imds/introduction). We plan to provide a tool to facilitate local testing in the future.

## Configuration

The SaladCloud Job Queue Worker automatically discovers the appropriate service endpoints when running on SaladCloud and no additional configuration is required.

The SaladCloud Job Queue Worker, by default, only prints error level log lines to minimize any potential noise in your workload logs. You may optionally override the log level to print more detailed log lines for monitoring or troubleshooting purposes. The default log level may be overridden using the `SALAD_LOG_LEVEL` environment variable. Valid values are `debug`, `info`, `warn`, and `error`. The SaladCloud Job Queue Worker will exit on startup if an invalid value is provided.

## Samples

See the [Mandelbrot workload sample](./samples/mandelbrot/) in the `samples/mandelbrot` directory for examples of different strategies that may be used to embed and run the SaladCloud Job Queue Worker in an existing workload container image.

## Development

The following prerequisites are required:

- [Visual Studio Code](https://code.visualstudio.com/download)
- [Git](https://git-scm.com/downloads)
- [Go](https://go.dev/dl/) (version 1.24.0 or higher)
- [golangci-lint](https://golangci-lint.run/welcome/install/) (version 2.0.0 or higher)
- [Buf](https://github.com/bufbuild/buf/releases/latest) (version 1.65.0 or higher)
- [protoc-gen-go](https://pkg.go.dev/google.golang.org/protobuf/cmd/protoc-gen-go), the Google protocol buffer compiler for Go
- [protoc-gen-go-grpc](https://pkg.go.dev/google.golang.org/grpc/cmd/protoc-gen-go-grpc), the gRPC service binding compiler for Go

1. Clone the repository.

   ```sh
   git clone https://github.com/SaladTechnologies/salad-cloud-job-queue-worker.git
   ```

2. Restore the dependencies.

   ```sh
   go mod download
   go mod verify
   ```

3. Build the project.

   ```sh
   make build
   ```

The build artifacts will be available in the `build` directory.
