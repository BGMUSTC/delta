package main

import (
	"fmt"

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

	if true {
		c := oplogs([]string{"123", "12", "111", "1112", "2435"}, nil)
		h := c.WriteTree(tw, sw)
		// for _, o := range c.Oplogs {
		// 	fmt.Println("o", o.Value(sw.Slots))
		// }
		trees = append(trees, h)

		fmt.Println("tree", 0)
		delta.DebugDfsTree(sw.Slots, h)
	}

	if true {
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
			fmt.Println("used", delta.ReadVarintArray(nil, sw.Slots, int(c.FullUsageOff)))
			// fmt.Println("tree", i)
			// delta.DebugDfsTree(sw.Slots, int(c.Tree))
		}
		if c.Flags&delta.TagCommitDiff != 0 {
			fmt.Println("diff used", delta.ReadVarintArray(nil, sw.Slots, int(c.DiffUsageOff)))
			// fmt.Println("diff tree", i)
			// delta.DebugDfsTree(sw.Slots, int(c.DiffTree))
		}
	}

	sw2, c2, _ := delta.Compact(sw.Slots, history, tw)
	fmt.Println("sw2", sw2.Slot0Len)
	delta.DebugDfsTree(sw2.Slots, int(c2.FullTree))
}

func testDelta() {
	// d := delta.NewDelta()
}

func testEmptyTree() {
	sw := delta.NewSnapshotWriter(0)
	emptytree := delta.WriteEmptyTree(sw)
	delta.Range(sw.Slots, emptytree, nil, func(k, v []byte) {
	})
}
