package usenet

import "github.com/sirrobot01/decypharr/pkg/storage"

// DecodeNZB decodes the bytes of a usenet/meta record (the v2 codec or a
// legacy protobuf file) without touching the live meta folder.
func DecodeNZB(data []byte) (*storage.NZB, error) { return decodeNZB(data) }
