# syntax=docker/dockerfile:1

FROM mandelbrot:latest

WORKDIR /app

ARG SALAD_JOB_QUEUE_WORKER_VERSION=0.5.0
ARG S6_OVERLAY_VERSION=3.2.1.0
ENV S6_KEEP_ENV=1

RUN apt-get update && \
    apt-get -y install xz-utils curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ADD https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz /tmp
RUN tar -C / -Jxpf /tmp/s6-overlay-noarch.tar.xz && \
    rm -rf /tmp/s6-overlay-noarch.tar.xz
ADD https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-x86_64.tar.xz /tmp
RUN tar -C / -Jxpf /tmp/s6-overlay-x86_64.tar.xz && \
    rm -rf /tmp/s6-overlay-x86_64.tar.xz
ADD https://github.com/SaladTechnologies/salad-cloud-job-queue-worker/releases/download/v${SALAD_JOB_QUEUE_WORKER_VERSION}/salad-http-job-queue-worker_x86_64.tar.gz /tmp
RUN tar -C /usr/local/bin -zxpf /tmp/salad-http-job-queue-worker_x86_64.tar.gz && \
    rm -rf /tmp/salad-http-job-queue-worker_x86_64.tar.gz

COPY --chmod=755 ./s6 /etc/

ENTRYPOINT ["/init"]
CMD ["/usr/local/bin/salad-http-job-queue-worker"]
