package allocator_test

import (
	"testing"

	"github.com/irfansharif/allocator"
	"github.com/stretchr/testify/require"
)

func TestAllocator(t *testing.T) {
	var rs []allocator.Range
	for i := 0; i < 3; i++ {
		rs = append(rs, allocator.NewRange(allocator.RangeID(i), 3, nil, nil))
	}

	var ns []allocator.Node
	for i := 0; i < 3; i++ {
		ns = append(ns, allocator.NewNode(allocator.NodeID(i), nil, nil))
	}
	al := allocator.New(rs, ns)
	ok, _ := al.Allocate()
	require.True(t, ok)
}
