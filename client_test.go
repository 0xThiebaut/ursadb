package ursadb

import (
	"encoding/json/v2"
	"slices"
	"testing"
)

var c = New("tcp://localhost:9281")

func TestClient_Ping(t *testing.T) {
	req, err := c.Ping(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	p, err := req.Wait()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("connection %s is %s with ursadb %s", p.Connection, p.Status, p.Version)
}

func TestClient_Compact_Smart(t *testing.T) {
	req, err := c.Compact(t.Context(), Smart)
	if err != nil {
		t.Fatal(err)
	}
	_, err = req.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClient_IndexPaths(t *testing.T) {
	taint := t.Name()

	// create a new dataset
	ireq, err := c.IndexPaths(t.Context(), []string{"/usr/bin"}, nil, []string{taint}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ireq.Wait()
	if err != nil {
		t.Fatal(err)
	}

	// recover the dataset
	treq, err := c.Topology(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	topo, err := treq.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if b, err := json.Marshal(topo.Datasets); err == nil {
		t.Log(string(b))
	}

	count := 0
	for dataset, entry := range topo.Datasets {
		if slices.Contains(entry.Taints, taint) {
			// drop the dataset
			dreq, err := c.DropDataset(t.Context(), dataset)
			if err != nil {
				t.Fatal(err)
			}
			_, err = dreq.Wait()
			if err != nil {
				t.Fatal(err)
			}
			count++
		}
	}

	if count != 1 {
		t.Fail()
	}
}

func TestClient_Compact_All(t *testing.T) {
	taint := t.Name()

	// create a first dataset
	ireq, err := c.IndexPaths(t.Context(), []string{"/usr/bin"}, nil, []string{taint}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ireq.Wait()
	if err != nil {
		t.Fatal(err)
	}

	// create a second dataset
	ireq, err = c.IndexPaths(t.Context(), []string{"/bin"}, nil, []string{taint}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ireq.Wait()
	if err != nil {
		t.Fatal(err)
	}

	// recover the pre-compact dataset
	treq, err := c.Topology(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	topo, err := treq.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if b, err := json.Marshal(topo.Datasets); err == nil {
		t.Log(string(b))
	}

	count := 0
	for _, entry := range topo.Datasets {
		if slices.Contains(entry.Taints, taint) {
			count++
		}
	}
	if count != 2 {
		t.Errorf("expected 2 datasets, got %d", count)
	}

	// merge the datasets
	creq, err := c.Compact(t.Context(), All)
	if err != nil {
		t.Fatal(err)
	}
	_, err = creq.Wait()
	if err != nil {
		t.Fatal(err)
	}

	// recover the post-compact dataset
	treq, err = c.Topology(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	topo, err = treq.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if b, err := json.Marshal(topo.Datasets); err == nil {
		t.Log(string(b))
	}

	count = 0
	for dataset, entry := range topo.Datasets {
		if slices.Contains(entry.Taints, taint) {
			// drop the dataset
			dreq, err := c.DropDataset(t.Context(), dataset)
			if err != nil {
				t.Fatal(err)
			}
			_, err = dreq.Wait()
			if err != nil {
				t.Fatal(err)
			}
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 dataset, got %d", count)
	}
}
