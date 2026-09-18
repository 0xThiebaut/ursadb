package ursadb

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/0xThiebaut/ursadb/internal"
	"github.com/pebbe/zmq4"
)

// Client interfaces with the ursadb API.
// While it is capable of issuing remote commands, the referenced filesystem paths are interpreted by the remote ursadb server.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L185-L194
type Client interface {
	// TaintDataset adds a tag to a dataset.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L186
	TaintDataset(ctx context.Context, dataset string, taint string) (Async[Ok], error)
	// UntaintDataset removes a tag from a dataset.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L186
	UntaintDataset(ctx context.Context, dataset string, taint string) (Async[Ok], error)
	// DropDataset removes a dataset from the database.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L186
	DropDataset(ctx context.Context, dataset string) (Async[Ok], error)
	// PopIterator reads from the iterator created with the `select into iterator` command.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L187
	PopIterator(ctx context.Context, iterator string) (Async[SelectFromIterator], error)
	// IndexPaths indexes remote directories recursively.
	// By default, a GRAM3 index type will be used.
	// It is recommended to use GRAM3, TEXT4, WIDE8 and, if space permits, HASH4 index types.
	// Files can be tagged immediately during indexing using the taints.
	// By default, every new file will be cross-checked with the current state of the database, to ensure that no duplicates are added.
	// Users can opt out of this (e.g., for performance reasons) with the insecure flag (i.e., nocheck).
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L188
	IndexPaths(ctx context.Context, remotes []string, types []Type, taints []string, insecure bool) (Async[Ok], error)
	// IndexList indexes targets from a remote list.
	// By default, a GRAM3 index type will be used.
	// It is recommended to use GRAM3, TEXT4, WIDE8 and, if space permits, HASH4 index types.
	// Files can be tagged immediately during indexing using the taints.
	// By default, every new file will be cross-checked with the current state of the database, to ensure that no duplicates are added.
	// Users can opt out of this (e.g., for performance reasons) with the insecure flag (i.e., nocheck).
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L188
	IndexList(ctx context.Context, remote string, types []Type, taints []string, insecure bool) (Async[Ok], error)
	// ReindexDataset adds a new index type to an existing dataset.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L189
	ReindexDataset(ctx context.Context, dataset string, types ...Type) (Async[Ok], error)
	// Compact forces database compacting; Datasets with different tags will never merge with each other.
	// The compacting method All forces the force compacting of all datasets into a single one.
	// The recommended compacting method Smart lets the database decide if it needs compacting or not.
	//
	// These commands will never create a dataset with more than `merge_max_files` files, and will never compact more than `merge_max_datasets` at once.
	// To ensure that the database is in a true minimal state, you may need to run the compact command multiple times.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L190
	Compact(ctx context.Context, method Compact) (Async[Ok], error)
	// GetConfig gets configuration variables.
	// When no keys are specified, GetConfig returns all values.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L191
	GetConfig(ctx context.Context, keys ...string) (Async[Config], error)
	// SetConfig changes a configuration variable.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L191
	SetConfig(ctx context.Context, key string, value int) (Async[Ok], error)
	// Status checks the status of tasks running in the database.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L192
	Status(ctx context.Context) (Async[Status], error)
	// Topology checks what datasets are loaded and which index types they use.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L193
	Topology(ctx context.Context) (Async[Topology], error)
	// Ping checks the database's availability.
	//
	// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L194
	Ping(ctx context.Context) (Async[Ping], error)
}

// New creates an ursadb Client for the given endpoint which is safe for concurrent use.
// Use Client.Ping to validate the database's availability.
func New(endpoint string) Client {
	return &client{
		Endpoint: endpoint,
	}
}

type client struct {
	Endpoint string
	wg       sync.WaitGroup
}

// socket creates a ZMQ socket aligning with ursadb's settings.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/src/Client.cpp#L143-L149
func socket(endpoint string) (*zmq4.Socket, error) {
	// create the status channel
	s, err := zmq4.NewSocket(zmq4.REQ)
	if err != nil {
		return nil, err
	}

	if err = s.SetLinger(0); err != nil {
		return nil, err
	}
	if err = s.SetRcvtimeo(1000); err != nil {
		return nil, err
	}

	return s, s.Connect(endpoint)
}

// receive waits for a ZMQ response and unmarshals it into the response.
func receive[T any](ctx context.Context, s *zmq4.Socket, t *response[T]) error {
	for ctx.Err() == nil {
		// get the message
		msg, err := s.RecvBytes(zmq4.DONTWAIT)
		if errors.Is(err, errAGAIN) {
			continue
		} else if err != nil {
			return err
		}

		if err = json.Unmarshal(msg, t); err != nil {
			return err
		} else if t.Error != nil {
			return t.Error
		}

		return nil
	}
	return ctx.Err()
}

// Command issues the provided command and unmarshals the typed response.
func (c *client) Command[T any](ctx context.Context, cmd string) (Async[T], error) {
	// create socket
	s, err := socket(c.Endpoint)
	if err != nil {
		return nil, err
	}

	// send a ping request for tracking purposes
	if _, err = s.SendBytes([]byte(ping), 0); err != nil {
		defer s.Close()
		return nil, err
	}

	// await the ping reply for tracking purposes
	var p response[Ping]
	if err = receive[Ping](ctx, s, &p); err != nil {
		defer s.Close()
		return nil, err
	}

	// prepare the asynchronous state
	closer := make(chan struct{})
	resp := &async[T]{
		response:   &response[T]{},
		done:       closer,
		connection: p.Result.Connection,
	}

	// avoid a duplicate ping
	if cmd == ping {
		if cast, ok := any(p).(response[T]); ok {
			defer s.Close()
			resp.response = &cast
			close(closer)
			return resp, nil
		}
	}

	// send the request
	if _, err = s.SendBytes([]byte(cmd), 0); err != nil {
		defer s.Close()
		return nil, err
	}

	go func() {
		defer s.Close()
		// await the response
		defer close(closer)
		resp.err = receive[T](ctx, s, resp.response)
	}()

	return resp, nil
}

// TaintDataset adds a tag to a dataset.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L186
func (c *client) TaintDataset(ctx context.Context, dataset string, taint string) (Async[Ok], error) {
	cmd := fmt.Sprintf("dataset %q taint %q;", dataset, taint)
	return c.Command[Ok](ctx, cmd)
}

// UntaintDataset removes a tag from a dataset.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L186
func (c *client) UntaintDataset(ctx context.Context, dataset string, taint string) (Async[Ok], error) {
	cmd := fmt.Sprintf("dataset %q untaint %q;", dataset, taint)
	return c.Command[Ok](ctx, cmd)
}

// DropDataset removes a dataset from the database.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L186
func (c *client) DropDataset(ctx context.Context, dataset string) (Async[Ok], error) {
	cmd := fmt.Sprintf("dataset %q drop;", dataset)
	return c.Command[Ok](ctx, cmd)
}

// PopIterator reads from the iterator created with the `select into iterator` command.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L187
func (c *client) PopIterator(ctx context.Context, iterator string) (Async[SelectFromIterator], error) {
	cmd := fmt.Sprintf("iterator %q pop;", iterator)
	return c.Command[SelectFromIterator](ctx, cmd)
}

// Shared logic for IndexPaths and IndexList
//
// By default, a GRAM3 index type will be used.
// It is recommended to use GRAM3, TEXT4, WIDE8 and, if space permits, HASH4 index types.
// Files can be tagged immediately during indexing using the taints.
// By default, every new file will be cross-checked with the current state of the database, to ensure that no duplicates are added.
// Users can opt out of this (e.g., for performance reasons) with the insecure flag (i.e., nocheck).
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L188
func (c *client) index(ctx context.Context, source string, types []Type, taints []string, insecure bool) (Async[Ok], error) {
	// validate arguments
	cast := make([]string, 0, len(types))
	for _, t := range types {
		if !t.Valid() {
			return nil, errors.New("invalid type")
		}
		cast = append(cast, string(t))
	}

	// index source
	var cmd strings.Builder
	if _, err := fmt.Fprintf(&cmd, "index %s", source); err != nil {
		return nil, err
	}

	// set types
	if len(types) > 0 {
		if _, err := fmt.Fprintf(&cmd, " with [%s]", strings.Join(cast, ", ")); err != nil {
			return nil, err
		}
	}

	// set taints
	if len(taints) > 0 {
		if _, err := fmt.Fprintf(&cmd, " with taints [%s]", strings.Join(internal.Map(taints, strconv.Quote), ", ")); err != nil {
			return nil, err
		}
	}

	// set insecure
	if insecure {
		if _, err := cmd.WriteString(" nocheck"); err != nil {
			return nil, err
		}
	}

	// close
	if err := cmd.WriteByte(';'); err != nil {
		return nil, err
	}

	return c.Command[Ok](ctx, cmd.String())
}

// IndexPaths indexes remote directories recursively.
// By default, a GRAM3 index type will be used.
// It is recommended to use GRAM3, TEXT4, WIDE8 and, if space permits, HASH4 index types.
// Files can be tagged immediately during indexing using the taints.
// By default, every new file will be cross-checked with the current state of the database, to ensure that no duplicates are added.
// Users can opt out of this (e.g., for performance reasons) with the insecure flag (i.e., nocheck).
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L188
func (c *client) IndexPaths(ctx context.Context, remotes []string, types []Type, taints []string, insecure bool) (Async[Ok], error) {
	if len(remotes) == 0 {
		return nil, errors.New("missing remote paths")
	}

	return c.index(ctx, strings.Join(internal.Map(remotes, strconv.Quote), " "), types, taints, insecure)
}

// IndexList indexes targets from a remote list.
// By default, a GRAM3 index type will be used.
// It is recommended to use GRAM3, TEXT4, WIDE8 and, if space permits, HASH4 index types.
// Files can be tagged immediately during indexing using the taints.
// By default, every new file will be cross-checked with the current state of the database, to ensure that no duplicates are added.
// Users can opt out of this (e.g., for performance reasons) with the insecure flag (i.e., nocheck).
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L188
func (c *client) IndexList(ctx context.Context, remote string, types []Type, taints []string, insecure bool) (Async[Ok], error) {
	return c.index(ctx, fmt.Sprintf("from list %q", remote), types, taints, insecure)
}

// ReindexDataset adds a new index type to an existing dataset.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L189
func (c *client) ReindexDataset(ctx context.Context, dataset string, types ...Type) (Async[Ok], error) {
	cast := make([]string, 0, len(types))
	for _, t := range types {
		if !t.Valid() {
			return nil, errors.New("invalid type")
		}
		cast = append(cast, string(t))
	}

	var cmd bytes.Buffer
	if _, err := fmt.Fprintf(&cmd, "reindex %q", dataset); err != nil {
		return nil, err
	}
	if len(types) > 0 {
		if _, err := fmt.Fprintf(&cmd, " with [%s]", strings.Join(cast, ", ")); err != nil {
			return nil, err
		}
	}
	if err := cmd.WriteByte(';'); err != nil {
		return nil, err
	}

	return c.Command[Ok](ctx, cmd.String())
}

// Compact forces database compacting.
// The compacting method All forces the force compacting of all datasets into a single one.
// The recommended compacting method Smart lets the database decide if it needs compacting or not.
//
// These commands will never create a dataset with more than `merge_max_files` files, and will never compact more than `merge_max_datasets` at once.
// To ensure that the database is in a true minimal state, you may need to run the compact command multiple times.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L190
func (c *client) Compact(ctx context.Context, method Compact) (Async[Ok], error) {
	if !method.Valid() {
		return nil, errors.New("invalid compact method")
	}
	cmd := fmt.Sprintf("compact %s;", method)
	return c.Command[Ok](ctx, cmd)
}

// GetConfig gets configuration variables.
// When no keys are specified, GetConfig returns all values.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L191
func (c *client) GetConfig(ctx context.Context, keys ...string) (Async[Config], error) {
	keys = internal.Map(keys, strconv.Quote)
	cmd := fmt.Sprintf("config get %s;", strings.Join(keys, " "))
	return c.Command[Config](ctx, cmd)
}

// SetConfig changes a configuration variable.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L191
func (c *client) SetConfig(ctx context.Context, key string, value int) (Async[Ok], error) {
	cmd := fmt.Sprintf("config set %s %d;", strconv.Quote(key), value)
	return c.Command[Ok](ctx, cmd)
}

// Status checks the status of tasks running in the database.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L192
func (c *client) Status(ctx context.Context) (Async[Status], error) {
	return c.Command[Status](ctx, "status;")
}

// Topology checks what datasets are loaded and which index types they use.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L193
func (c *client) Topology(ctx context.Context) (Async[Topology], error) {
	return c.Command[Topology](ctx, "topology;")
}

// Ping checks the database's availability.
//
// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/QueryParser.cpp#L194
func (c *client) Ping(ctx context.Context) (Async[Ping], error) {
	return c.Command[Ping](ctx, ping)
}
