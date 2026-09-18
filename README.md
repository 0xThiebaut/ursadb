# ursadb

A Go client for [CERT Polska's ursadb](https://github.com/CERT-Polska/ursadb) trigram database, suited for malware indexing.

Currently, the client implements all management features except querying (i.e., `select` statements).
The client is aimed at easing management and indexing while querying can leverage [CERT Polska's mquery](https://github.com/CERT-Polska/mquery) project.

```go
// create a client
client := New("tcp://localhost:9281")

// issue the command
req, err := client.Ping(context.Background())
if err != nil {
    log.Fatalln(err)
}

// wait synchronously for the asynchronous result
ping, err := req.Wait()
if err != nil {
    log.Fatalln(err)
}

// connection 006B8B45E4 is ok with ursadb 1.5.3
log.Printf("connection %s is %s with ursadb %s\n", ping.Connection, ping.Status, ping.Version)
```

## Dependencies

This library relies on [`pebbe/zmq4`](github.com/pebbe/zmq4) which is a `cgo` wrapper around `libzmq`.
Running this client hence requires the [installation of `libzmq`](https://zeromq.org/download/).

Furthermore, being a `cgo` wrapper, building the client requires a Go installation [compatible with `cgo`](https://pkg.go.dev/cmd/cgo).

---

Licensed under the EUPL