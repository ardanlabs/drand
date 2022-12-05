package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func rtb(byteOrder binary.ByteOrder, r uint64) []byte {
	var buff bytes.Buffer
	_ = binary.Write(&buff, byteOrder, r)
	return buff.Bytes()
}

func rtb2(byteOrder binary.ByteOrder, r uint64) []byte {
	out := make([]byte, 8)
	byteOrder.PutUint64(out, r)
	return out
}

var v []byte

func doRtb(byteOrder binary.ByteOrder, count int) {
	val := rtb(byteOrder, uint64(count))
	v = val
}

func doRtb2(byteOrder binary.ByteOrder, count int) {
	val := rtb2(byteOrder, uint64(count))
	v = val
}

func BenchmarkBigEndian(b *testing.B) {
	for i := 0; i < b.N; i++ {
		doRtb(binary.BigEndian, i)
	}
	_ = v
}

func BenchmarkLittleEndian(b *testing.B) {
	for i := 0; i < b.N; i++ {
		doRtb(binary.LittleEndian, i)
	}
	_ = v
}

func BenchmarkBigEndian2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		doRtb2(binary.BigEndian, i)
	}
	_ = v
}

func BenchmarkLittleEndian2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		doRtb2(binary.LittleEndian, i)
	}
	_ = v
}
