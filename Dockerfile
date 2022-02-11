FROM ubuntu
ENV DEBIAN_FRONTEND noninteractive
RUN apt update && apt install -y git iputils-ping iproute2 tcpdump netcat vim dnsutils curl \
    && apt clean && rm -rf /var/lib/apt/lists/*
