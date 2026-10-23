package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/pem"
	"fmt"
	"testing"
)

func TestFilterBundle(t *testing.T) {
	blockedDER, keptDER := []byte("blocked"), []byte("kept")
	var source bytes.Buffer
	for _, der := range [][]byte{blockedDER, keptDER} {
		if err := pem.Encode(&source, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
			t.Fatal(err)
		}
	}
	fingerprint := sha256.Sum256(blockedDER)
	result, removed, err := filterBundle(source.Bytes(), map[string]bool{fmt.Sprintf("%x", fingerprint): true})
	if err != nil || removed != 1 {
		t.Fatalf("removed=%d, err=%v", removed, err)
	}
	block, rest := pem.Decode(result)
	if block == nil || !bytes.Equal(block.Bytes, keptDER) || len(bytes.TrimSpace(rest)) != 0 {
		t.Fatal("filtered bundle did not retain exactly the unblocked certificate")
	}
	if _, _, err := filterBundle([]byte("not PEM"), blockedRoots); err == nil {
		t.Fatal("malformed bundle accepted")
	}
}
