package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/pem"
	"fmt"
	"os"
)

// Match certificate DER fingerprints, regardless of labels in the source bundle.
var blockedRoots = map[string]bool{
	"d7a7a0fb5d7e2731d771e9484ebcdef71d5f0c3e0a2948782bc83ee0ea699ef4": true, // COMODO AAA
	"86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b": true, // emSign ECC
	"7dd78d5f4f13459a83dff9abbba62edbaf6f2d102bf257fd712f4d9f2746ed8d": true, // emSign TLS
	"f2edaaff3d9b701fb4d14c9f285f8c0bad90fc3bcc1fa07d47f4718edaba34a5": true, // emSign code signing
}

func filterBundle(data []byte, blocked map[string]bool) ([]byte, int, error) {
	var out bytes.Buffer
	count, removed := 0, 0
	for len(bytes.TrimSpace(data)) > 0 {
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, 0, fmt.Errorf("invalid certificate bundle")
		}
		count++
		data = rest
		fingerprint := sha256.Sum256(block.Bytes)
		if blocked[fmt.Sprintf("%x", fingerprint)] {
			removed++
			continue
		}
		if err := pem.Encode(&out, block); err != nil {
			return nil, 0, err
		}
	}
	if count == 0 || count == removed {
		return nil, 0, fmt.Errorf("certificate bundle has no usable roots")
	}
	return out.Bytes(), removed, nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: ca-bundle-filter INPUT OUTPUT")
		os.Exit(2)
	}
	input, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	output, removed, err := filterBundle(input, blockedRoots)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(os.Args[2], output, 0644); err != nil {
		panic(err)
	}
	fmt.Printf("removed %d distrusted roots\n", removed)
}
