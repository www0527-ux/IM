package idgen

import "testing"

func TestNewRejectsInvalidNodeID(t *testing.T) {
	for _, nodeID := range []int64{0, 1024} {
		if _, err := New(Config{NodeID: nodeID}); err == nil {
			t.Fatalf("New(%d) succeeded, want error", nodeID)
		}
	}
}

func TestGeneratorsUseIndependentNodeIDs(t *testing.T) {
	first, err := New(Config{NodeID: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(Config{NodeID: 2})
	if err != nil {
		t.Fatal(err)
	}

	firstID, err := first.NextID()
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := second.NextID()
	if err != nil {
		t.Fatal(err)
	}
	if firstID == secondID {
		t.Fatalf("different nodes generated the same id: %d", firstID)
	}
}
