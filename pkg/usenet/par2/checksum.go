package par2

import (
	"crypto/md5"
	"hash/crc32"
)

// md5AndCRC32 computes the MD5 and CRC32 (IEEE polynomial, the standard PAR2
// uses) of data in one pass, in the shape IFSC packets record per slice.
func md5AndCRC32(data []byte) SliceChecksum {
	return SliceChecksum{
		MD5:   md5.Sum(data),
		CRC32: crc32.ChecksumIEEE(data),
	}
}
