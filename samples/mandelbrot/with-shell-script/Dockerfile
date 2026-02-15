# syntax=docker/dockerfile:1

FROM mandelbrot:latest

WORKDIR /app

ARG SALAD_JOB_QUEUE_WORKER_VERSION=0.6.0

RUN apt-get update && \
    apt-get -y install curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ADD https://github.com/SaladTechnologies/salad-cloud-job-queue-worker/releases/download/v${SALAD_JOB_QUEUE_WORKER_VERSION}/salad-http-job-queue-worker_x86_64.tar.gz /tmp
RUN tar -C /usr/local/bin -zxpf /tmp/salad-http-job-queue-worker_x86_64.tar.gz && \
    rm -rf /tmp/salad-http-job-queue-worker_x86_64.tar.gz

COPY --chmod=755 ./run.sh ./

CMD ["/app/run.sh"]
