# Dockerfile References: https://docs.docker.com/engine/reference/builder/

# Start from golang v1.11 base image
FROM golang:1.11

# Add Maintainer Info
LABEL maintainer="Amir Razmjou <arazmj@gmail.com>"

# Set the Current Working Directory inside the container
WORKDIR $GOPATH/src/firewall_events

# Copy everything from the current directory to the PWD(Present Working Directory) inside the container
COPY . .

WORKDIR event_sink

RUN ls -la 

# Download all the dependencies
# https://stackoverflow.com/questions/28031603/what-do-three-dots-mean-in-go-command-line-invocations
RUN go get -d -v 

# Install the package
RUN go install -v 

RUN go build 

# Run the executable
CMD ["event_sink"]