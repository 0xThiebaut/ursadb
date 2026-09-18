package ursadb

import (
	"syscall"

	"github.com/pebbe/zmq4"
)

var (
	errAGAIN = zmq4.AsErrno(syscall.EAGAIN)
)
