package main

import (
	"fmt"

	"github.com/nareix/delta"
)

func main() {
	tw := &delta.TreeWriter{}
	trees := []int{}

	set := func(c *delta.DiffTreeWriter, s string) {
		c.WriteOplogSet([]byte(s), []byte(s))
	}

	sw := delta.NewSnapshotRewriter(nil)

	if true {
		c := &delta.DiffTreeWriter{}
		c.Reset()
		c.Start()
		set(c, "123")
		set(c, "12")
		set(c, "111")
		set(c, "1112")
		set(c, "2435")
		c.End()

		sw.OrigSlots = c.Slots
		delta.BuildCommitTree(tw, c.Slots, c.Oplogs)

		h := tw.Write(sw)
		trees = append(trees, h)

		fmt.Println("tree", 0)
		delta.DebugDfsTree(sw.Slots, h)
	}

	if true {
		c := &delta.DiffTreeWriter{}
		c.Reset()
		c.Start()
		set(c, "433")
		set(c, "443")
		set(c, "4")
		set(c, "119")
		set(c, "120")
		c.WriteOplogRemove([]byte("123"))
		c.End()

		sw.OrigSlots = c.Slots
		delta.BuildCommitTree(tw, c.Slots, c.Oplogs)

		h := tw.Write(sw)
		trees = append(trees, h)

		fmt.Println("tree", 1)
		delta.DebugDfsTree(sw.Slots, h)
	}

	{
		sw.OrigSlots = sw.Slots
		delta.MergeTree(sw.Slots, trees[0], trees[1], tw)
		h := tw.Write(sw)
		trees = append(trees, h)
	}

	fmt.Println("tree", 2)
	delta.DebugDfsTree(sw.Slots, trees[2])

	delta.Range(sw.Slots, trees[2], []byte(""), func(k, v []byte) {
		fmt.Println("k", string(k), "v", string(v))
	})
}
