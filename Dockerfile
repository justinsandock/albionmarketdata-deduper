FROM golang:1.8
MAINTAINER Regner Blok-Andersen <shadowdf@gmail.com>

ADD . /go/src/github.com/regner/albiondata-deduper
WORKDIR /go/src/github.com/regner/albiondata-deduper

RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure
RUN go install

ENTRYPOINT /go/bin/albiondata-deduper