package main

import (
	"fmt"

	"github.com/nareix/delta"
)

func oplogs(sw *delta.SnapshotWriter, set, remove []string) *delta.DiffTreeWriter {
	c := &delta.DiffTreeWriter{W: sw}
	for _, k := range set {
		c.WriteOplogSet([]byte(k), []byte(k))
	}
	for _, k := range remove {
		c.WriteOplogRemove([]byte(k))
	}
	return c
}

func testSimple() {
	sw := delta.NewSnapshotWriter(0)
	tw := &delta.TreeWriter{}
	trees := []int{}

	set := func(c *delta.DiffTreeWriter, s string) {
		c.WriteOplogSet([]byte(s), []byte(s))
	}

	if true {
		c := &delta.DiffTreeWriter{W: sw}
		set(c, "123")
		set(c, "12")
		set(c, "111")
		set(c, "1112")
		set(c, "2435")

		h := c.WriteTree(tw)
		trees = append(trees, h)

		fmt.Println("tree", 0)
		delta.DebugDfsTree(sw.Slots, h)
	}

	if true {
		c := &delta.DiffTreeWriter{W: sw}
		set(c, "433")
		set(c, "443")
		set(c, "4")
		set(c, "119")
		set(c, "120")
		c.WriteOplogRemove([]byte("123"))

		h := c.WriteTree(tw)
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
	used := &delta.Used{}
	c0 := delta.Commit{Flags: delta.TagCommitTree}
	c0.Tree = int32(tree)
	parent := c0.Write(sw)

	commit := func(set, remove []string) {
		diff := oplogs(sw, set, remove)
		c := delta.Commit{}
		c.Flags = delta.TagCommitTree | delta.TagCommitParent | delta.TagCommitDiff
		c.DiffTree = int32(diff.WriteTree(tw))
		c.DiffUsedOff = int32(sw.WriteVarintArray(diff.Used.Used))
		used.Add(diff.Used)
		tree = delta.MergeTree(sw.Slots, tree, int(c.DiffTree), used, tw, sw)
		c.Tree = int32(tree)
		c.UsedOff = int32(sw.WriteVarintArray(used.Used))
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
		if c.Flags&delta.TagCommitTree != 0 {
			fmt.Println("used", delta.ReadVarintArray(nil, sw.Slots, int(c.UsedOff)))
			// fmt.Println("tree", i)
			// delta.DebugDfsTree(sw.Slots, int(c.Tree))
		}
		if c.Flags&delta.TagCommitDiff != 0 {
			fmt.Println("diff used", delta.ReadVarintArray(nil, sw.Slots, int(c.DiffUsedOff)))
			// fmt.Println("diff tree", i)
			// delta.DebugDfsTree(sw.Slots, int(c.DiffTree))
		}
	}

	sw2, c2off := delta.Compact(sw.Slots, history, tw)
	c2 := delta.ReadCommit(sw2.Slots, c2off)
	fmt.Println("sw2", sw2.Slot0Len)
	delta.DebugDfsTree(sw2.Slots, int(c2.Tree))
}

func testEmptyTree() {
	sw := delta.NewSnapshotWriter(0)
	emptytree := delta.WriteEmptyTree(sw)
	delta.Range(sw.Slots, emptytree, nil, func(k, v []byte) {
	})
}

func main() {
	testCompact()
}
