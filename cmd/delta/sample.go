package main

import (
	"bufio"
	"fmt"
	"io"

	"github.com/nareix/delta"
)

func oplogs(set, remove []string) *delta.DiffTreeWriter {
	c := &delta.DiffTreeWriter{}
	c.Reset()
	for _, k := range set {
		c.Set([]byte(k), []byte(k))
	}
	for _, k := range remove {
		c.Remove([]byte(k))
	}
	return c
}

func testSimple() {
	sw := delta.NewSnapshotWriter(0)
	tw := &delta.TreeWriter{}
	trees := []int{}

	{
		c := oplogs([]string{"123", "12", "111", "1112", "2435"}, nil)
		h := c.WriteTree(tw, sw)
		trees = append(trees, h)

		fmt.Println("tree", 0)
		delta.DebugDfsTree(sw.Slots, h)
	}

	{
		c := oplogs([]string{"433", "443", "4", "119", "120"}, []string{"123"})
		h := c.WriteTree(tw, sw)
		trees = append(trees, h)

		fmt.Println("tree", 1)
		delta.DebugDfsTree(sw.Slots, h)
	}

	{
		h := delta.MergeTree(sw.Slots, trees[0], trees[1], nil, tw, sw)
		trees = append(trees, h)
	}

	fmt.Println("tree", 2)
	delta.DebugDfsTree(sw.Slots, trees[2])

	delta.Range(sw.Slots, trees[2], []byte(""), func(k, v []byte) {
		fmt.Println("k", string(k), "v", string(v))
	})
}

func testCompact() {
	sw := delta.NewSnapshotWriter(0)
	tw := &delta.TreeWriter{}

	tree := delta.WriteEmptyTree(sw)
	usage := &delta.Usage{}
	c0 := delta.Commit{Flags: delta.TagCommitFull}
	c0.FullTree = int32(tree)
	parent := c0.Write(sw)

	commit := func(set, remove []string) {
		diff := oplogs(set, remove)
		c := delta.Commit{}
		c.Flags = delta.TagCommitFull | delta.TagCommitParent | delta.TagCommitDiff
		c.DiffTree = int32(diff.WriteTree(tw, sw))
		c.DiffUsageOff = int32(sw.WriteVarintArray(diff.Usage.Usage))
		usage.Add(diff.Usage)
		tree = delta.MergeTree(sw.Slots, tree, int(c.DiffTree), usage, tw, sw)
		c.FullTree = int32(tree)
		c.FullUsageOff = int32(sw.WriteVarintArray(usage.Usage))
		c.Parent = int32(parent)
		c.SlotsLenOff = int32(sw.WriteVarintArray(sw.SlotsLen))
		parent = c.Write(sw)
	}

	commit([]string{"1", "2", "3"}, nil)
	commit([]string{"4", "5", "6"}, []string{"1", "2"})
	commit([]string{"7", "8", "9"}, nil)

	history := delta.ReadHistory(sw.Slots, parent, 2)
	for i, c := range history {
		fmt.Println("history", i)
		if c.Flags&delta.TagCommitFull != 0 {
			fmt.Println("usage", delta.ReadVarintArray(nil, sw.Slots, int(c.FullUsageOff)))
		}
		if c.Flags&delta.TagCommitDiff != 0 {
			fmt.Println("diff usage", delta.ReadVarintArray(nil, sw.Slots, int(c.DiffUsageOff)))
		}
	}

	sw2, c2, _ := delta.Compact(sw.Slots, history, tw)
	delta.DebugDfsTree(sw2.Slots, int(c2.FullTree))
}

type PipeReadWriter struct {
	*io.PipeReader
	*io.PipeWriter
}

func newConn2() (PipeReadWriter, PipeReadWriter) {
	r, w := io.Pipe()
	r1, w1 := io.Pipe()
	c := PipeReadWriter{PipeReader: r, PipeWriter: w1}
	c1 := PipeReadWriter{PipeReader: r1, PipeWriter: w}
	return c, c1
}

func newBufioRW(rw PipeReadWriter) *bufio.ReadWriter {
	return bufio.NewReadWriter(
		bufio.NewReaderSize(rw, 128),
		bufio.NewWriterSize(rw, 128),
	)
}

func testEmptyTree() {
	sw := delta.NewSnapshotWriter(0)
	emptytree := delta.WriteEmptyTree(sw)
	delta.Range(sw.Slots, emptytree, nil, func(k, v []byte) {
	})
}

func ExampleDelta() {
	d0 := delta.NewDelta()
	d1 := delta.NewDelta()
	d2 := delta.NewDelta()

	{
		c0, c1 := newConn2()
		go d0.HandleConn(c0)
		go d1.RequestSubscribeConn(c1)
	}

	{
		c0, c1 := newConn2()
		go d1.HandleConn(c0)
		go d2.RequestSubscribeConn(c1)
	}

	{
		c0, c1 := newConn2()
		go d0.HandleConn(c0)
		p := delta.NewSyncProto(newBufioRW(c1))
		p.Role = delta.SPRolePulish
		p.WriteHeader()

		p.WriteTreeStart(delta.SPTypeDiff)
		p.WriteOplog(delta.OpSet, []byte("123"), []byte("123"))
		p.WriteTreeEnd()

		p.WriteTreeStart(delta.SPTypeDiff)
		p.WriteOplog(delta.OpSet, []byte("456"), []byte("456"))
		p.WriteTreeEnd()
	}

	{
		c0, c1 := newConn2()
		go d2.HandleConn(c0)
		p := delta.NewSyncProto(newBufioRW(c1))
		p.Role = delta.SPRoleSubscribe
		p.Version = 1
		p.WriteHeader()
		for i := 0; i < 2; i++ {
			typ, _ := p.ReadTreeStart()
			switch typ {
			case delta.SPTypeFull:
				fmt.Println("full")
				p.ReadTreeVersion()
			case delta.SPTypeDiff:
				fmt.Println("diff")
				p.ReadTree()
				for _, o := range p.DW.Oplogs {
					if o.ValueOp == delta.OpSet {
						fmt.Println("k", string(o.Key(p.DW.Slots)), "v", string(o.Value(p.DW.Slots)))
					}
				}
			default:
				panic("invalid")
			}
		}
	}
}
