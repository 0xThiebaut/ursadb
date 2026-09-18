package ursadb

import "time"

type response[T any] struct {
	Counters Counters `json:"counters,omitempty"`
	Error    *Error   `json:"error,omitempty"`
	Result   *T       `json:"result,omitempty"`
}

type Connection interface {
	// Connection provides the asynchronous connection identifier, it can be used to check a Task's Status.
	Connection() string
}

// Async represents an asynchronously running Task.
// The task can be made blocking through Wait, or its status periodically checked using its Connection within a Status check.
type Async[T any] interface {
	Connection
	// Done returns a channel that gets closed once the Task completes, successfully or not.
	Done() <-chan struct{}
	// Err returns any error that encountered during the Task execution.
	Err() error
	// Result returns a successful Task result.
	Result() *T
	// Counters returns a successful Task's Counters, if any.
	Counters() Counters
	// Wait waits for Done to complete, returning the Result and Err that might have resulted from the Task processing.
	Wait() (*T, error)
}

type async[T any] struct {
	connection string
	done       <-chan struct{}
	err        error
	response   *response[T]
}

// Connection provides the asynchronous connection identifier, it can be used to check a Task's Status.
func (a *async[T]) Connection() string {
	return a.connection
}

// Done returns a channel that gets closed once the Task completes, successfully or not.
func (a *async[T]) Done() <-chan struct{} {
	return a.done
}

// Err returns any error that encountered during the Task execution.
func (a *async[T]) Err() error {
	return a.err
}

// Result returns a successful Task result.
func (a *async[T]) Result() *T {
	return a.response.Result
}

// Counters returns a successful Task's Counters, if any.
func (a *async[T]) Counters() Counters {
	return a.response.Counters
}

// Wait waits for Done to complete, returning the Result and Err that might have resulted from the Task processing.
func (a *async[T]) Wait() (*T, error) {
	<-a.Done()
	return a.Result(), a.Err()
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L5-L11
type Counter struct {
	Count        int `json:"count"`
	Milliseconds int `json:"milliseconds"`
}

func (c Counter) Duration() time.Duration {
	return time.Millisecond * time.Duration(c.Milliseconds)
}

type Counters map[string]Counter

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L13-L23
type Select struct {
	Mode  string   `json:"mode"`
	Files []string `json:"files"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L25-L34
type SelectFromIterator struct {
	Mode     string   `json:"mode"`
	Files    []string `json:"files"`
	Position int      `json:"iterator_position"`
	Total    int      `json:"total_files"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L36-L47
type SelectIterator struct {
	Mode     string `json:"mode"`
	Count    int    `json:"file_count"`
	Iterator int    `json:"iterator"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L49-L53
type Ok struct {
	Status string `json:"status"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L55-L61
type Ping struct {
	Status     string `json:"status"`
	Connection string `json:"connection_id"`
	Version    string `json:"ursadb_version"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L63-L68
type Error struct {
	Message string `json:"message"`
	Retry   bool   `json:"retry"`
}

func (e Error) Error() string {
	return e.Message
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L70-L88
type Topology struct {
	Datasets map[string]DatasetEntry `json:"datasets"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L73-L85
type DatasetEntry struct {
	Indexes []IndexEntry `json:"indexes"`
	Size    int          `json:"size"`
	Files   int          `json:"file_count"`
	Taints  []string     `json:"taints"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L75-L81
type IndexEntry struct {
	Type string `json:"type"`
	Size int    `json:"size"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L90-L107
type Status struct {
	Tasks   []Task `json:"tasks"`
	Version string `json:"ursadb_version"`
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L93-L102
type Task struct {
	ID         int    `json:"id"`
	Connection string `json:"connection_id"`
	Request    string `json:"request"`
	Done       int    `json:"work_done"`
	Estimated  int    `json:"work_estimated"`
	Epoch      int    `json:"epoch_ms"`
}

func (t Task) Time() time.Time {
	return time.UnixMilli(int64(t.Epoch))
}

// https://github.com/CERT-Polska/ursadb/blob/9dce7ddbc002560016732fe7474906936313b802/libursa/Responses.cpp#L109-L113
type Config struct {
	Keys map[string]int `json:"keys"`
}
