package delta

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	TagCommitFull = 1 << iota
	TagCommitDiff
	TagCommitParent
)

const (
	TagNodeKey = 1 << iota
	TagNodeValue
	TagNodeRef
	TagNodeChild
	tagNodeMerge      // inner use
	tagNodeCheckValid // inner use
)

const (
	OpSet    = 1
	OpRemove = 2
)

const (
	debugBuildCommitTree = false
	debugMergeTree       = false
	debugCompact         = false
	debugDelta           = false
)

var ErrProto = fmt.Errorf("proto error")
var ErrInvalidPublish = fmt.Errorf("invalid publish")

func getInt(b []byte, n int) int {
	v := 0
	for i := 0; i < n; i++ {
		v <<= 8
		v |= int(b[i])
	}
	return v
}

func putInt(b []byte, v int, n int) {
	for i := 0; i < n; i++ {
		b[n-1-i] = byte(v)
		v >>= 8
	}
}

func uvarintSize(x int) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

func writeUvarint(w *Buffer, v int) {
	size := uvarintSize(v)
	binary.PutUvarint(w.NextBytes(size), uint64(v))
}

func chstr(ch int) string {
	if ch == -1 {
		return "-1"
	}
	return string([]byte{byte(ch)})
}

type ProtoReader struct {
	B   []byte
	Off int
}

func (r *ProtoReader) Reset(b []byte) {
	r.B = b
	r.Off = 0
}

func (r *ProtoReader) ReadU8() byte {
	if r.Off < len(r.B) {
		b := r.B[r.Off]
		r.Off++
		return b
	}
	return 0
}

func (r *ProtoReader) ReadByte() (byte, error) {
	b := r.ReadU8()
	if r.EOF() {
		return b, io.EOF
	} else {
		return b, nil
	}
}

func (r *ProtoReader) ReadUvarint() int {
	v, err := binary.ReadUvarint(r)
	if err != nil {
		panic(ErrProto)
	}
	return int(v)
}

func (r *ProtoReader) ReadBytes(n int) []byte {
	if r.Off+n < len(r.B) {
		b := r.B[r.Off : r.Off+n]
		r.Off += n
		return b
	}
	return nil
}

func (r *ProtoReader) Skip(n int) {
	r.Off += n
}

func (r *ProtoReader) EOF() bool {
	return r.Off >= len(r.B)
}

type Node struct {
	Flags      byte
	OffIsAbs   bool
	Off        int32
	ChildStart int32
	ChildNr    int32
	RefOff     int32
	KeyOff     int32
	KeyLen     int32
	ValueOp    int32
	ValueSlot  int32
	ValueOff   int32
	ValueLen   int32
	ValidNr    int32
}

func (n Node) Value(slots [][]byte) []byte {
	return slots[int(n.ValueSlot)][int(n.ValueOff):int(n.ValueOff+n.ValueLen)]
}

func (n Node) Key(slots [][]byte) []byte {
	return slots[0][int(n.KeyOff):int(n.KeyOff+n.KeyLen)]
}

func oplogKey(slots [][]byte, o Node) []byte {
	return o.Key(slots)
}

func sortOplog(slots [][]byte, oplogs []Node) {
	sort.Slice(oplogs, func(i, j int) bool {
		r := bytes.Compare(oplogKey(slots, oplogs[i]), oplogKey(slots, oplogs[j]))
		if r != 0 {
			return r < 0
		} else {
			return oplogs[i].Off < oplogs[j].Off
		}
	})
}

func searchOplogs(tw *TreeWriter, slots [][]byte, oplogs []Node, ni int, oplogstart, oplogend int) bool {
	n := &tw.Nodes[ni]
	last := -2
	cont := 0
	levelEnd := n.KeyOff + n.KeyLen

	for oi := oplogstart; oi <= oplogend; oi++ {
		if debugBuildCommitTree {
			if oi < oplogend {
				key := oplogKey(slots, oplogs[oi])
				fmt.Println("searchOplog", oi, "key", string(key), "at", levelEnd)
			}
		}

		var cur int
		if oi == oplogend {
			cur = -2
		} else if levelEnd == oplogs[oi].KeyLen {
			cur = -1
		} else {
			key := oplogKey(slots, oplogs[oi])
			cur = int(key[levelEnd])
		}
		if cur != last {
			if last == -2 {
			} else if last == -1 {
				o := oplogs[oi-1]
				nodeCopyValue(n, o)
				if debugBuildCommitTree {
					fmt.Println("set value", "ni", ni, "value", string(n.Value(slots)))
				}
			} else {
				if debugBuildCommitTree {
					fmt.Println("cont", cont, "ch", string([]byte{byte(last)}))
				}
				if cont == oplogend-oplogstart {
					n.KeyLen++
					return true
				} else {
					tw.Nodes = append(tw.Nodes, Node{
						Flags:      TagNodeChild | TagNodeKey,
						ChildStart: int32(oi - cont),
						ChildNr:    int32(cont),
						KeyOff:     int32(levelEnd),
						KeyLen:     1,
					})
					n = &tw.Nodes[ni]
					n.ChildNr++
					if debugBuildCommitTree {
						fmt.Println(ni, "append", len(tw.Nodes)-1)
					}
				}
			}
			cont = 1
		} else {
			cont++
		}
		last = cur
	}
	return false
}

var emptyTree = []byte{0}

func WriteEmptyTree(sw *SnapshotWriter) int {
	return sw.WriteTree(emptyTree)
}

func BuildCommitTree(tw *TreeWriter, slots [][]byte, oplogs []Node) {
	sortOplog(slots, oplogs)

	tw.Nodes = tw.Nodes[:0]
	tw.Nodes = append(tw.Nodes, Node{
		Flags:      TagNodeChild,
		ChildStart: 0,
		ChildNr:    int32(len(oplogs)),
	})

	if debugBuildCommitTree {
		fmt.Println("oplogs")
		for i := 0; i < len(oplogs); i++ {
			fmt.Println(string(oplogKey(slots, oplogs[i])), oplogs[i].ValueOp, "keyoff", oplogs[i].KeyOff)
		}
		fmt.Println("bfs")
	}

	for ni := 0; ni < len(tw.Nodes); ni++ {
		n := &tw.Nodes[ni]
		o := oplogs[int(n.ChildStart)]
		if n.ChildNr == 1 {
			nodeCopyValue(n, o)
			levelStart := n.KeyOff
			n.KeyOff = o.KeyOff + levelStart
			n.KeyLen = o.KeyLen - levelStart
			n.Flags |= TagNodeKey
			n.ChildNr = 0
		} else {
			oplogstart := int(n.ChildStart)
			oplogend := int(n.ChildStart + n.ChildNr)
			n.ChildStart = int32(len(tw.Nodes))
			n.ChildNr = 0
			for searchOplogs(tw, slots, oplogs, ni, oplogstart, oplogend) {
			}
			n := &tw.Nodes[ni]
			levelStart := n.KeyOff
			n.KeyOff = o.KeyOff + levelStart
			n.Flags |= TagNodeKey
		}
	}
}

type TreeWriter struct {
	Nodes []Node
	treeb Buffer
}

func tryPtrsize(tw *TreeWriter, ptrsize int, n *Node) bool {
	off := int32(tw.treeb.Len()) + 2
	for ci := n.ChildStart; ci < n.ChildStart+int32(n.ChildNr); ci++ {
		c := tw.Nodes[ci]
		if n.Flags&tagNodeCheckValid != 0 && n.ValidNr == 0 {
			continue
		}
		off++
		ptr := off - c.Off
		if uvarintSize(int(ptr)) > ptrsize {
			return false
		}
		off += int32(ptrsize)
	}
	return true
}

func (tw *TreeWriter) Write(slots [][]byte, w *SnapshotWriter) int {
	if debugBuildCommitTree {
		fmt.Println("write")
	}

	tw.treeb.Reset()

	for ni := len(tw.Nodes) - 1; ni >= 0; ni-- {
		n := &tw.Nodes[ni]

		if n.Flags&(TagNodeKey|TagNodeRef|TagNodeChild|TagNodeValue) == 0 {
			continue
		}

		n.Off = int32(tw.treeb.Len())
		n.OffIsAbs = false

		if debugBuildCommitTree {
			fmt.Println("node", ni, "off", n.Off, "keyoff", n.KeyOff, "keylen", n.KeyLen, "childstart", n.ChildStart, "childnr", n.ChildNr)
		}

		flags := byte(0)
		flagsoff := tw.treeb.Len()
		tw.treeb.NextBytes(1)

		// key
		if n.Flags&TagNodeKey != 0 && n.KeyLen > 0 {
			flags |= TagNodeKey
			writeUvarint(&tw.treeb, int(n.KeyLen))
			key := slots[0][int(n.KeyOff):int(n.KeyOff+n.KeyLen)]
			tw.treeb.Write(key)
			if debugBuildCommitTree {
				fmt.Println(" key", string(key))
			}
		}

		// ref
		if n.Flags&TagNodeRef != 0 {
			flags |= TagNodeRef
			writeUvarint(&tw.treeb, int(n.RefOff))
		}

		// value
		if n.Flags&TagNodeValue != 0 {
			flags |= TagNodeValue
			tw.treeb.WriteByte(byte(n.ValueOp))
			if debugBuildCommitTree {
				fmt.Println(" op", n.ValueOp)
			}
			if n.ValueOp == OpSet {
				writeUvarint(&tw.treeb, int(n.ValueSlot)) // slot
				writeUvarint(&tw.treeb, int(n.ValueOff))  // off
				writeUvarint(&tw.treeb, int(n.ValueLen))  // size
				if debugBuildCommitTree {
					value := n.Value(w.Slots)
					fmt.Println(" value", value, "off", "slot", "len", n.ValueOff, n.ValueSlot, n.ValueLen)
				}
			}
		}

		// child
		if n.Flags&TagNodeChild != 0 && n.ChildNr > 0 {
			flags |= TagNodeChild

			ptrsize := 1
			for ptrsize < 4 {
				if tryPtrsize(tw, ptrsize, n) {
					break
				}
				ptrsize++
			}

			tw.treeb.WriteByte(byte(ptrsize))
			childnroff := tw.treeb.Len()
			tw.treeb.WriteByte(0)
			childnr := 0

			for ci := n.ChildStart; ci < n.ChildStart+int32(n.ChildNr); ci++ {
				c := tw.Nodes[ci]
				if n.Flags&tagNodeCheckValid != 0 && c.ValidNr == 0 {
					continue
				}
				ch := slots[0][int(c.KeyOff)]
				tw.treeb.WriteByte(ch)
				off := int(c.Off)
				if c.OffIsAbs {
					off = -(w.Slot0Len - off)
				}
				ptr := tw.treeb.Len() - off
				putInt(tw.treeb.NextBytes(ptrsize), ptr, ptrsize)
				if debugBuildCommitTree {
					fmt.Println(" index", "ch", chstr(int(ch)), "ptr", off)
				}
				childnr++
			}

			tw.treeb.Bytes()[childnroff] = byte(childnr - 1)
		}

		tw.treeb.Bytes()[flagsoff] = flags

		if debugBuildCommitTree {
			fmt.Println(" flags", fmt.Sprintf("%x", flags))
		}
	}

	off := w.Slot0Len
	w.WriteTree(tw.treeb.Bytes())

	if debugBuildCommitTree {
		fmt.Print(hex.Dump(tw.treeb.Bytes()))
	}

	start := int(tw.Nodes[0].Off) + int(off)
	return start
}

func ReadNode(t []byte, off int) Node {
	n := Node{
		Off:      int32(off),
		OffIsAbs: true,
	}

	r := ProtoReader{B: t, Off: off}
	flags := r.ReadU8()
	n.Flags = flags

	if flags&TagNodeKey != 0 {
		n.KeyLen = int32(r.ReadUvarint())
		n.KeyOff = int32(r.Off)
		r.Skip(int(n.KeyLen))
	}

	if flags&TagNodeRef != 0 {
		refoff := r.ReadUvarint()
		refn := ReadNode(t, refoff)
		refn.KeyOff = n.KeyOff
		refn.KeyLen = n.KeyLen
		return refn
	}

	if flags&TagNodeValue != 0 {
		n.ValueOp = int32(r.ReadU8())
		if n.ValueOp == OpSet {
			n.ValueSlot = int32(r.ReadUvarint())
			n.ValueOff = int32(r.ReadUvarint())
			n.ValueLen = int32(r.ReadUvarint())
		}
	}

	if flags&TagNodeChild != 0 {
		n.ChildStart = int32(r.Off)
		ptrsize := int(r.ReadU8())
		count := int(r.ReadU8()) + 1
		r.Skip((ptrsize + 1) * count)
	}

	return n
}

type ChildReader struct {
	b       []byte
	off     int
	ptrsize int
	count   int
	pos     int
}

func InitChildReader(b []byte, off int) ChildReader {
	if off == 0 {
		return ChildReader{}
	}
	r := ChildReader{
		b:   b,
		off: off + 2,
	}
	r.ptrsize = int(r.b[off])
	r.count = int(r.b[off+1]) + 1
	return r
}

func (r *ChildReader) End() bool {
	return r.pos == r.count
}

func (r *ChildReader) Peek() (int, int) {
	ch := int(r.b[r.off])
	off := r.off + 1
	ptr := off - getInt(r.b[off:off+r.ptrsize], r.ptrsize)
	return ch, ptr
}

func (r *ChildReader) Next() (int, int) {
	if r.End() {
		return -1, -1
	}
	ch, ptr := r.Peek()
	r.off += r.ptrsize + 1
	r.pos++
	return ch, ptr
}

func (r *ChildReader) Find(findch byte) int {
	// TODO: binary search
	for {
		ch, ptr := r.Next()
		if ch == -1 {
			return -1
		}
		if ch == int(findch) {
			return ptr
		}
	}
}

type MergeNode struct {
	Off0    int32
	Off1    int32
	KeyOff0 int32
	KeyOff1 int32
	KeyLen0 int32
	KeyLen1 int32
}

func FromMergeNode(n MergeNode) Node {
	return Node{
		Flags:      tagNodeMerge,
		Off:        n.Off0,
		ChildStart: n.Off1,
		ChildNr:    n.KeyOff0,
		RefOff:     n.KeyOff1,
		KeyOff:     n.KeyLen0,
		KeyLen:     n.KeyLen1,
	}
}

func ToMergeNode(n Node) MergeNode {
	return MergeNode{
		Off0:    n.Off,
		Off1:    n.ChildStart,
		KeyOff0: n.ChildNr,
		KeyOff1: n.RefOff,
		KeyLen0: n.KeyOff,
		KeyLen1: n.KeyLen,
	}
}

type mergeReader interface {
	End() bool
	Peek() (int, Node)
	Next()
}

type mergeChildReader struct {
	cr    ChildReader
	slots [][]byte
}

func (mr *mergeChildReader) End() bool {
	return mr.cr.End()
}

func (mr *mergeChildReader) Peek() (int, Node) {
	ch, ptr := mr.cr.Peek()
	n := ReadNode(mr.slots[0], ptr)
	return ch, Node{
		Off:      n.Off,
		OffIsAbs: true,
		KeyOff:   n.KeyOff,
		KeyLen:   n.KeyLen,
	}
}

func (mr *mergeChildReader) Next() {
	mr.cr.Next()
}

func newMergeChildReader(slots [][]byte, n Node) *mergeChildReader {
	return &mergeChildReader{
		slots: slots,
		cr:    InitChildReader(slots[0], int(n.ChildStart)),
	}
}

type mergeSingleNodeReader struct {
	slots [][]byte
	n     Node
	end   bool
}

func (mr *mergeSingleNodeReader) End() bool {
	return mr.end
}

func (mr *mergeSingleNodeReader) Peek() (int, Node) {
	key := mr.n.Key(mr.slots)
	return int(key[0]), mr.n
}

func (mr *mergeSingleNodeReader) Next() {
	mr.end = true
}

func mergeChildren(slots [][]byte, cr0, cr1 mergeReader, tw *TreeWriter) {
	for {
		if cr0.End() && cr1.End() {
			break
		} else if !cr0.End() && !cr1.End() {
			ch0, n0 := cr0.Peek()
			ch1, n1 := cr1.Peek()
			if ch0 < ch1 {
				if debugMergeTree {
					fmt.Println(" merge child ch0", chstr(ch0), "off0", n0.Off)
				}
				tw.Nodes = append(tw.Nodes, n0)
				cr0.Next()
			} else if ch0 == ch1 {
				if debugMergeTree {
					fmt.Println(" merge child ch0", chstr(ch0), "off0", n0.Off, "ch1", chstr(ch1), "off1", n1.Off)
				}
				tw.Nodes = append(tw.Nodes, FromMergeNode(MergeNode{
					Off0:    n0.Off,
					KeyOff0: n0.KeyOff,
					KeyLen0: n0.KeyLen,
					Off1:    n1.Off,
					KeyOff1: n1.KeyOff,
					KeyLen1: n1.KeyLen,
				}))
				cr0.Next()
				cr1.Next()
			} else {
				if debugMergeTree {
					fmt.Println(" merge child ch1", chstr(ch1), "off1", n1.Off)
				}
				tw.Nodes = append(tw.Nodes, n1)
				cr1.Next()
			}
		} else if !cr0.End() {
			ch0, n0 := cr0.Peek()
			if debugMergeTree {
				fmt.Println(" merge child ch0", chstr(ch0), "off0", n0.Off)
			}
			tw.Nodes = append(tw.Nodes, n0)
			cr0.Next()
		} else {
			ch1, n1 := cr1.Peek()
			if debugMergeTree {
				fmt.Println(" merge child ch1", chstr(ch1), "off1", n1.Off)
			}
			tw.Nodes = append(tw.Nodes, n1)
			cr1.Next()
		}
	}
}

func nodeCopyValue(dst *Node, src Node) {
	dst.Flags |= TagNodeValue
	dst.ValueOp = src.ValueOp
	dst.ValueSlot = src.ValueSlot
	dst.ValueOff = src.ValueOff
	dst.ValueLen = src.ValueLen
}

func newMergeReader(slots [][]byte, ch int, n Node, ki int) mergeReader {
	if ch == -1 {
		return newMergeChildReader(slots, n)
	} else {
		refn := Node{
			Flags:  TagNodeKey | TagNodeRef,
			Off:    n.Off,
			RefOff: n.Off,
			KeyOff: n.KeyOff + int32(ki),
			KeyLen: n.KeyLen - int32(ki),
		}
		return &mergeSingleNodeReader{slots: slots, n: refn}
	}
}

func mergeTwo(slots [][]byte, n0, n1 Node, usage *Usage, tw *TreeWriter) Node {
	if debugMergeTree {
		fmt.Println("merge two", "n0", n0.Off, "key0", string(n0.Key(slots)), "n1", n1.Off, "key1", string(n1.Key(slots)))
	}

	ki := 0
	for {
		var ch0, ch1 int
		if ki < int(n0.KeyLen) {
			ch0 = int(slots[0][int(n0.KeyOff)+ki])
		} else {
			ch0 = -1
		}
		if ki < int(n1.KeyLen) {
			ch1 = int(slots[0][int(n1.KeyOff)+ki])
		} else {
			ch1 = -1
		}

		if debugMergeTree {
			fmt.Println(" ch0", chstr(ch0), "ch1", chstr(ch1))
		}

		if ch0 == -1 || ch1 == -1 || ch0 != ch1 {
			n := Node{
				Flags:      TagNodeKey | TagNodeChild,
				KeyOff:     n0.KeyOff,
				KeyLen:     int32(ki),
				ChildStart: int32(len(tw.Nodes)),
			}
			cr0 := newMergeReader(slots, ch0, n0, ki)
			cr1 := newMergeReader(slots, ch1, n1, ki)
			mergeChildren(slots, cr0, cr1, tw)
			n.ChildNr = int32(len(tw.Nodes)) - n.ChildStart
			n0value := false
			n1value := false
			if ch0 == -1 {
				if n0.Flags&TagNodeValue != 0 {
					nodeCopyValue(&n, n0)
					n0value = true
				}
			}
			if ch1 == -1 {
				if n1.Flags&TagNodeValue != 0 {
					nodeCopyValue(&n, n1)
					n1value = true
				}
			}
			if n0value && n1value {
				if n0.ValueOp == OpSet {
					usage.AddValue(int(n0.ValueSlot), -int(n0.ValueLen))
					if n1.ValueOp == OpSet {
						usage.AddValue(int(n1.ValueSlot), int(n1.ValueLen))
					}
				}
			}
			if debugMergeTree {
				fmt.Println(" copy value")
			}
			return n
		}
		ki++
	}
}

func MergeTree(slots [][]byte, tree0, tree1 int, usage *Usage, tw *TreeWriter, sw *SnapshotWriter) int {
	t := slots[0]

	if usage == nil {
		usage = &Usage{}
	}

	tw.Nodes = tw.Nodes[:0]
	tw.Nodes = append(tw.Nodes, FromMergeNode(MergeNode{
		Off0: int32(tree0),
		Off1: int32(tree1),
	}))

	for ni := 0; ni < len(tw.Nodes); ni++ {
		n := tw.Nodes[ni]

		if n.Flags&tagNodeMerge != 0 {
			m := ToMergeNode(n)
			n0 := ReadNode(t, int(m.Off0))
			n1 := ReadNode(t, int(m.Off1))
			tw.Nodes[ni] = mergeTwo(slots, n0, n1, usage, tw)
		}
	}

	return tw.Write(slots, sw)
}

func Get(slots [][]byte, off int, k []byte) (bool, []byte) {
	t := slots[0]
	n := ReadNode(t, off)
	ki := 0
	for ki < len(k) {
		if n.KeyLen > 0 {
			if t[n.KeyOff] == k[ki] {
				ki++
				n.KeyOff++
				n.KeyLen--
			} else {
				return false, nil
			}
		} else {
			cr := InitChildReader(t, int(n.ChildStart))
			ptr := cr.Find(k[ki])
			if ptr == -1 {
				return false, nil
			}
			n = ReadNode(t, ptr)
		}
	}
	if n.KeyLen == 0 {
		if n.Flags&TagNodeValue != 0 {
			if n.ValueOp == OpSet {
				return true, n.Value(slots)
			}
		}
	}
	return false, nil
}

type rangeDfs struct {
	all    bool
	slots  [][]byte
	prefix []byte
	fn     func(k, v []byte)
	fn2    func(op int, k, v []byte) error
	n      Node
}

func (d *rangeDfs) search() error {
	key := d.n.Key(d.slots)
	d.prefix = append(d.prefix, key...)
	if d.n.Flags&TagNodeValue != 0 {
		if d.all {
			if err := d.fn2(int(d.n.ValueOp), d.prefix, d.n.Value(d.slots)); err != nil {
				return err
			}
		} else {
			if d.n.ValueOp == OpSet {
				d.fn(d.prefix, d.n.Value(d.slots))
			}
		}
	}
	cr := InitChildReader(d.slots[0], int(d.n.ChildStart))
	for !cr.End() {
		_, ptr := cr.Next()
		d.n = ReadNode(d.slots[0], ptr)
		if err := d.search(); err != nil {
			return err
		}
	}
	d.prefix = d.prefix[:len(d.prefix)-len(key)]
	return nil
}

func Range(slots [][]byte, off int, prefix []byte, fn func(k, v []byte)) []byte {
	t := slots[0]
	n := ReadNode(t, off)
	pi := 0
	for pi < len(prefix) {
		if n.KeyLen > 0 {
			if t[n.KeyOff] == prefix[pi] {
				pi++
				n.KeyOff++
				n.KeyLen--
				continue
			} else {
				return prefix
			}
		} else {
			cr := InitChildReader(t, int(n.ChildStart))
			ptr := cr.Find(prefix[pi])
			if ptr == -1 {
				return prefix
			}
			n = ReadNode(t, ptr)
		}
	}
	d := rangeDfs{
		slots:  slots,
		prefix: prefix,
		fn:     fn,
		n:      n,
	}
	d.search()
	return d.prefix
}

func debugPrintDepth(depth int) {
	fmt.Print(strings.Repeat(" ", depth*2))
}

func debugDfsTree(slots [][]byte, off int, depth int) {
	n := ReadNode(slots[0], off)

	debugPrintDepth(depth)
	fmt.Println("off", n.Off)

	if n.Flags&TagNodeKey != 0 {
		debugPrintDepth(depth)
		fmt.Println("key", string(n.Key(slots)))
	}

	if n.Flags&TagNodeValue != 0 {
		debugPrintDepth(depth)
		fmt.Println("op", n.ValueOp)
		if n.ValueOp == OpSet {
			debugPrintDepth(depth)
			fmt.Println("value", string(n.Value(slots)))
		}
	}

	if n.ChildStart != 0 {
		cr := InitChildReader(slots[0], int(n.ChildStart))
		for !cr.End() {
			ch, ptr := cr.Next()
			debugPrintDepth(depth)
			fmt.Println("index", "ch", chstr(int(ch)), "ptr", ptr)
			debugDfsTree(slots, ptr, depth+1)
		}
	}
}

func DebugDfsTree(slots [][]byte, off int) {
	fmt.Println("dfs")
	debugDfsTree(slots, off, 0)
}

type Usage struct {
	Usage []int
}

func (u *Usage) Reset() {
	u.Usage = u.Usage[:0]
}

func (u *Usage) prepare(slot int) {
	for len(u.Usage) <= slot {
		u.Usage = append(u.Usage, 0)
	}
}

func (u *Usage) AddValue(slot, vlen int) {
	u.prepare(slot)
	u.Usage[slot] += vlen
}

func (u *Usage) Add(u2 Usage) {
	u.prepare(len(u2.Usage) - 1)
	for i := 0; i < len(u2.Usage); i++ {
		u.Usage[i] += u2.Usage[i]
	}
}

func sumLen(a []int) int {
	n := 0
	for _, v := range a {
		n += v
	}
	return n
}

func RewriteTree(slots [][]byte, off int, remove bool, slotsremap []int, usage *Usage, tw *TreeWriter, sw *SnapshotWriter) int {
	n := ReadNode(slots[0], off)

	tw.Nodes = tw.Nodes[:0]
	tw.Nodes = append(tw.Nodes, n)

	for i := 0; i < len(tw.Nodes); i++ {
		n := &tw.Nodes[i]
		if n.Flags&TagNodeChild != 0 {
			childstart := len(tw.Nodes)
			cr := InitChildReader(slots[0], int(n.ChildStart))
			for !cr.End() {
				_, ptr := cr.Next()
				cn := ReadNode(slots[0], ptr)
				tw.Nodes = append(tw.Nodes, cn)
			}
			tw.Nodes[i].ChildStart = int32(childstart)
			tw.Nodes[i].ChildNr = int32(len(tw.Nodes) - childstart)
		}
	}

	if remove {
		for i := len(tw.Nodes) - 1; i >= 0; i-- {
			n := &tw.Nodes[i]
			if n.Flags&TagNodeValue != 0 {
				if n.ValueOp == OpSet {
					n.ValidNr++
				}
			}
			if n.Flags&TagNodeChild != 0 {
				n.Flags |= tagNodeCheckValid
				for ci := n.ChildStart; ci < n.ChildStart+n.ChildNr; ci++ {
					n.ValidNr += tw.Nodes[ci].ValidNr
				}
			}
		}
	}

	if debugCompact {
		fmt.Println("rewrite tree", "off", off, "slotsremap", slotsremap)
	}

	for i := len(tw.Nodes) - 1; i >= 0; i-- {
		n := &tw.Nodes[i]
		if n.Flags&TagNodeValue != 0 {
			if n.ValueOp == OpSet {
				slot := slotsremap[int(n.ValueSlot)]
				if slot == -1 {
					// copy
					newslot, newoff, b := sw.NextValueCopy(int(n.ValueLen))
					copy(b, n.Value(slots))
					n.ValueSlot = int32(newslot)
					n.ValueOff = int32(newoff)
					usage.AddValue(newslot, int(n.ValueLen))
				} else {
					// no copy
					usage.AddValue(slot, int(n.ValueLen))
				}
			}
		}
	}

	return tw.Write(slots, sw)
}

func ReadHistory(slots [][]byte, head int, maxnr int) []Commit {
	history := []Commit{}
	for {
		c := ReadCommit(slots, head)
		history = append(history, c)
		if maxnr != -1 && len(history) >= maxnr {
			break
		}
		if !c.HasParent() {
			break
		}
		head = int(c.Parent)
	}
	return history
}

func HistoryUsage(slots [][]byte, history []Commit) *Usage {
	oldest := len(history) - 1
	latest := 0

	usage := &Usage{
		Usage: ReadVarintArray(nil, slots, int(history[oldest].FullUsageOff)),
	}
	for i := oldest - 1; i >= latest; i-- {
		diffusage := Usage{
			Usage: ReadVarintArray(nil, slots, int(history[i].DiffUsageOff)),
		}
		usage.Add(diffusage)
	}

	return usage
}

func Compact(slots [][]byte, history []Commit, tw *TreeWriter) (*SnapshotWriter, Commit, *Usage) {
	if len(history) < 2 {
		panic("invalid")
	}
	historyusage := HistoryUsage(slots, history)

	oldest := len(history) - 1
	latest := 0

	slotslen := ReadVarintArray(nil, slots, int(history[latest].SlotsLenOff))

	slotsremap := make([]int, len(slotslen))
	nocopynr := 0
	for i := 1; i < len(historyusage.Usage); i++ {
		if float64(historyusage.Usage[i])/float64(slotslen[i]) > CompactNoCopyThresold {
			slotsremap[i] = i // no copy
			nocopynr++
		} else {
			slotsremap[i] = -1 // copy
		}
	}

	if debugCompact {
		fmt.Println("compact", "history nr", len(history))
	}

	sw := NewSnapshotWriter(nocopynr)
	for i := 1; i < nocopynr+1; i++ {
		sw.Slots[i] = slots[i]
	}

	usage := &Usage{}
	history[oldest].Flags = TagCommitFull
	history[oldest].FullTree = int32(RewriteTree(slots, int(history[oldest].FullTree), true, slotsremap, usage, tw, sw))
	history[oldest].FullUsageOff = int32(sw.WriteVarintArray(usage.Usage))
	history[oldest].SlotsLenOff = int32(sw.WriteVarintArray(sw.SlotsLen))
	history[oldest].Off = int32(history[oldest].Write(sw))

	for i := oldest - 1; i >= latest; i-- {
		diffusage := Usage{}
		history[i].Flags = TagCommitFull | TagCommitDiff | TagCommitParent
		history[i].DiffTree = int32(RewriteTree(slots, int(history[i].DiffTree), false, slotsremap, &diffusage, tw, sw))
		history[i].DiffUsageOff = int32(sw.WriteVarintArray(diffusage.Usage))
		tree := MergeTree(sw.Slots, int(history[i+1].FullTree), int(history[i].DiffTree), usage, tw, sw)
		usage.Add(diffusage)
		history[i].FullTree = int32(tree)
		history[i].FullUsageOff = int32(sw.WriteVarintArray(usage.Usage))
		history[i].SlotsLenOff = int32(sw.WriteVarintArray(sw.SlotsLen))
		history[i].Parent = int32(history[i+1].Off)
		history[i].Off = int32(history[i].Write(sw))
	}

	return sw, history[latest], usage
}

var CompactTotalUsageThresold = 0.5
var CompactNoCopyThresold = 0.8

func ShouldCompact(slots [][]byte, head int) (bool, []Commit) {
	history := []Commit{}
	now := time.Now()
	for {
		c := ReadCommit(slots, head)
		ctime := time.Unix(c.Time, 0)
		if now.Sub(ctime) > time.Second {
			break
		}
		if len(history) > 32 {
			break
		}
		history = append(history, c)
		if !c.HasParent() {
			break
		}
		head = int(c.Parent)
	}
	if len(history) < 2 {
		return false, nil
	}

	latest := 0
	usage := HistoryUsage(slots, history)
	slotslen := ReadVarintArray(nil, slots, int(history[latest].SlotsLenOff))
	totallen := sumLen(slotslen)
	totalusage := sumLen(usage.Usage)
	if float64(totalusage)/float64(totallen) > CompactTotalUsageThresold {
		return false, nil
	}

	return true, history
}

type Commit struct {
	Off          int32
	Flags        byte
	Version      int32
	Time         int64
	SlotsLenOff  int32
	FullTree     int32
	FullUsageOff int32
	DiffTree     int32
	DiffUsageOff int32
	Parent       int32
}

func (c Commit) HasParent() bool {
	return c.Flags&TagCommitParent != 0
}

func ReadCommit(slots [][]byte, off int) Commit {
	r := ProtoReader{B: slots[0], Off: off}
	c := Commit{Off: int32(off)}
	c.Flags = r.ReadU8()
	c.Version = int32(r.ReadUvarint())
	c.Time = int64(r.ReadUvarint())
	c.SlotsLenOff = int32(r.ReadUvarint())
	if c.Flags&TagCommitFull != 0 {
		c.FullTree = int32(r.ReadUvarint())
		c.FullUsageOff = int32(r.ReadUvarint())
	}
	if c.Flags&TagCommitDiff != 0 {
		c.DiffTree = int32(r.ReadUvarint())
		c.DiffUsageOff = int32(r.ReadUvarint())
	}
	if c.Flags&TagCommitParent != 0 {
		c.Parent = int32(r.ReadUvarint())
	}
	return c
}

func (c Commit) Write(sw *SnapshotWriter) int {
	b := &Buffer{}
	b.WriteByte(c.Flags)
	writeUvarint(b, int(c.Version))
	writeUvarint(b, int(c.Time))
	writeUvarint(b, int(c.SlotsLenOff))
	if c.Flags&TagCommitFull != 0 {
		writeUvarint(b, int(c.FullTree))
		writeUvarint(b, int(c.FullUsageOff))
	}
	if c.Flags&TagCommitDiff != 0 {
		writeUvarint(b, int(c.DiffTree))
		writeUvarint(b, int(c.DiffUsageOff))
	}
	if c.Flags&TagCommitParent != 0 {
		writeUvarint(b, int(c.Parent))
	}
	return sw.WriteTree(b.Bytes())
}

func ReadVarintArray(a []int, slots [][]byte, off int) []int {
	r := ProtoReader{B: slots[0], Off: off}
	n := r.ReadUvarint()
	for i := 0; i < n; i++ {
		v := r.ReadUvarint()
		a = append(a, v)
	}
	return a
}

var DefaultValueSlotSize = 1024 * 16
var DefaultTreeSlotSize = 1024 * 16

type SnapshotWriter struct {
	Slots    [][]byte
	SlotsLen []int
	Slot0Len int
	WSlot    int
	b        Buffer
}

func NewSnapshotWriter(emptySlotNr int) *SnapshotWriter {
	w := &SnapshotWriter{
		Slots:    make([][]byte, 2+emptySlotNr, 16),
		SlotsLen: make([]int, 2+emptySlotNr, 16),
		WSlot:    emptySlotNr + 1,
	}
	w.Slots[0] = make([]byte, DefaultTreeSlotSize)
	w.Slots[w.WSlot] = make([]byte, DefaultValueSlotSize)
	w.Slot0Len = 0
	return w
}

func (w *SnapshotWriter) NextValueCopy(n int) (int, int, []byte) {
	for {
		if w.SlotsLen[w.WSlot]+n <= len(w.Slots[w.WSlot]) {
			break
		}
		w.WSlot++
		if w.WSlot == len(w.Slots) {
			w.Slots = append(w.Slots, make([]byte, DefaultValueSlotSize))
			w.SlotsLen = append(w.SlotsLen, 0)
		}
	}
	slot := w.WSlot
	off := w.SlotsLen[w.WSlot]
	b := w.Slots[slot][off : off+n]
	w.SlotsLen[w.WSlot] += n
	return slot, off, b
}

func (w *SnapshotWriter) AppendSlot(b []byte) int {
	slot := len(w.Slots)
	w.Slots = append(w.Slots, b)
	w.SlotsLen = append(w.SlotsLen, len(b))
	return slot
}

func (w *SnapshotWriter) newSlots() {
	newslots := make([][]byte, len(w.Slots), cap(w.Slots))
	copy(newslots, w.Slots)
	w.Slots = newslots
}

func (w *SnapshotWriter) WriteTree(b []byte) int {
	off := w.Slot0Len
	if n := int(w.Slot0Len) + len(b); n > len(w.Slots[0]) {
		w.newSlots()
		newslot0 := make([]byte, n*2)
		copy(newslot0, w.Slots[0])
		w.Slots[0] = newslot0
	}
	copy(w.Slots[0][int(w.Slot0Len):], b)
	w.Slot0Len += len(b)
	return off
}

func (w *SnapshotWriter) WriteVarintArray(a []int) int {
	w.b.Reset()
	writeUvarint(&w.b, len(a))
	for _, v := range a {
		writeUvarint(&w.b, v)
	}
	return w.WriteTree(w.b.Bytes())
}

type Snapshot struct {
	Slots   [][]byte
	HeadOff int
}

type DiffTreeWriter struct {
	Usage  Usage
	b      Buffer
	Oplogs []Node
	Slots  [][]byte
}

func (c *DiffTreeWriter) Reset() {
	c.b.Reset()
	c.Oplogs = c.Oplogs[:0]
	c.Slots = c.Slots[:0]
	c.Slots = append(c.Slots, nil)
}

func (c *DiffTreeWriter) Set(k, v []byte) {
	keyoff, k2 := c.NextKey(len(k))
	copy(k2, k)
	valueslot, valueoff, v2 := c.NextValue(len(v))
	copy(v2, v)
	c.AppendOplog(Node{
		KeyOff:    int32(keyoff),
		KeyLen:    int32(len(k)),
		ValueOp:   OpSet,
		ValueSlot: int32(valueslot),
		ValueOff:  int32(valueoff),
		ValueLen:  int32(len(v)),
	})
}

func (c *DiffTreeWriter) Remove(k []byte) {
	keyoff, k2 := c.NextKey(len(k))
	copy(k2, k)
	c.AppendOplog(Node{
		KeyOff:  int32(keyoff),
		KeyLen:  int32(len(k)),
		ValueOp: OpRemove,
	})
}

func (c *DiffTreeWriter) AppendOplog(n Node) {
	n.Off = int32(len(c.Oplogs))
	c.Oplogs = append(c.Oplogs, n)
	c.Slots[0] = c.b.Bytes()
}

func (c *DiffTreeWriter) NextKey(n int) (int, []byte) {
	off := c.b.Len()
	return off, c.b.NextBytes(n)
}

func (c *DiffTreeWriter) NextValue(n int) (int, int, []byte) {
	if n < DefaultValueSlotSize {
		off := c.b.Len()
		return 0, off, c.b.NextBytes(n)
	} else {
		b := make([]byte, n)
		slot := len(c.Slots)
		c.Slots = append(c.Slots, b)
		return slot, 0, b
	}
}

func (c *DiffTreeWriter) WriteTree(tw *TreeWriter, sw *SnapshotWriter) int {
	c.Slots[0] = c.b.Bytes()
	defer func() {
		for i := 0; i < len(c.Slots); i++ {
			c.Slots[i] = nil
		}
	}()

	c.Usage.Reset()
	for i := 0; i < len(c.Oplogs); i++ {
		n := &c.Oplogs[i]
		if n.ValueOp == OpSet {
			if n.ValueSlot == 0 {
				slot, off, b := sw.NextValueCopy(int(n.ValueLen))
				copy(b, n.Value(c.Slots))
				n.ValueSlot = int32(slot)
				n.ValueOff = int32(off)
			} else {
				slot := sw.AppendSlot(c.Slots[int(n.ValueSlot)])
				n.ValueSlot = int32(slot)
			}
			c.Usage.AddValue(int(n.ValueSlot), int(n.ValueLen))
		}
	}

	BuildCommitTree(tw, c.Slots, c.Oplogs)
	return tw.Write(c.Slots, sw)
}

// SyncProto v1
// Header: [0x01(1B)][Role(1B)]
// Oplog: [Op(1B)][KeyLen(Varint)][Key][ValueLen(Varint)][Value]
// Diff: [TypeDiff(1B)][Oplog][Oplog]...[0]
// Tree: [TypeFull(1B)][Version(Varint)][Oplog][Oplog]...[0]
// Publish: ->[Header][Diff][Diff]...
// subscribe: ->[Header][Version(Varint)] <-[Full][Diff]...[Full][Diff]

// usage:
// client(publish):
//   p := SyncProto{Role: SPRolePublish}; p.WriteHeader(); p.WriteTreeStart(); p.WriteOplog(); p.WriteTreeEnd()
// client(subscribe):
//   p := SyncProto{Role: SPRoleSubscribe, Version: 2}; p.WriteHeader(); p.ReadTree(); p.TreeType; p.DW.Oplogs;

const (
	SPTypeDiff      = 1
	SPTypeFull      = 2
	SPRolePulish    = 1
	SPRoleSubscribe = 2
)

type SyncProto struct {
	rw      *bufio.ReadWriter
	uw      UvarintWriter
	Role    int
	Version int
	DW      DiffTreeWriter
}

func NewSyncProto(rw *bufio.ReadWriter) *SyncProto {
	return &SyncProto{
		rw: rw,
		uw: UvarintWriter{W: rw},
	}
}

type UvarintWriter struct {
	W io.Writer
	b [16]byte
}

func (w *UvarintWriter) WriteUvarint(v int) error {
	n := uvarintSize(v)
	b := w.b[:n]
	binary.PutUvarint(b, uint64(v))
	_, err := w.W.Write(b)
	return err
}

func (p *SyncProto) ReadHeader() error {
	p.rw.ReadByte()
	role, err := p.rw.ReadByte()
	if err != nil {
		return err
	}
	switch role {
	case SPRolePulish:
	case SPRoleSubscribe:
		version, err := binary.ReadUvarint(p.rw)
		if err != nil {
			return err
		}
		p.Version = int(version)
	default:
		return ErrProto
	}
	p.Role = int(role)
	return nil
}

func (p *SyncProto) WriteHeader() error {
	if err := p.rw.WriteByte(0x01); err != nil {
		return err
	}
	if err := p.rw.WriteByte(byte(p.Role)); err != nil {
		return err
	}
	switch p.Role {
	case SPRolePulish:
	case SPRoleSubscribe:
		if err := p.uw.WriteUvarint(p.Version); err != nil {
			return err
		}
	}
	return p.rw.Flush()
}

func (p *SyncProto) WriteTreeVersion(version int) error {
	if err := p.uw.WriteUvarint(version); err != nil {
		return err
	}
	return nil
}

func (p *SyncProto) ReadTreeVersion() (int, error) {
	version, err := binary.ReadUvarint(p.rw)
	if err != nil {
		return -1, err
	}
	return int(version), nil
}

func (p *SyncProto) WriteOplog(op int, k, v []byte) error {
	if err := p.rw.WriteByte(byte(op)); err != nil {
		return err
	}
	if err := p.uw.WriteUvarint(len(k)); err != nil {
		return err
	}
	if _, err := p.rw.Write(k); err != nil {
		return err
	}
	switch op {
	case OpSet:
		if err := p.uw.WriteUvarint(len(v)); err != nil {
			return err
		}
		if _, err := p.rw.Write(v); err != nil {
			return err
		}
	case OpRemove:
	default:
		panic("invalid")
	}
	return nil
}

func (p *SyncProto) WriteTreeStart(typ int) error {
	return p.rw.WriteByte(byte(typ))
}

func (p *SyncProto) ReadTreeStart() (int, error) {
	typ, err := p.rw.ReadByte()
	if err != nil {
		return -1, err
	}
	return int(typ), nil
}

func (p *SyncProto) WriteTreeEnd() error {
	if err := p.rw.WriteByte(0); err != nil {
		return err
	}
	return p.rw.Flush()
}

func (p *SyncProto) WriteTree(slots [][]byte, off int) error {
	d := rangeDfs{
		all:   true,
		slots: slots,
		fn2:   p.WriteOplog,
		n:     ReadNode(slots[0], off),
	}
	return d.search()
}

func (p *SyncProto) ReadTree() error {
	p.DW.Reset()
	for {
		op, err := p.rw.ReadByte()
		if err != nil {
			return err
		}
		if op == 0 {
			return nil
		}
		n := Node{ValueOp: int32(op)}
		keylen, err := binary.ReadUvarint(p.rw)
		if err != nil {
			return err
		}
		keyoff, key := p.DW.NextKey(int(keylen))
		if _, err := io.ReadFull(p.rw, key); err != nil {
			return err
		}
		n.KeyLen = int32(keylen)
		n.KeyOff = int32(keyoff)
		switch op {
		case OpRemove:
			p.DW.AppendOplog(n)
		case OpSet:
			valuelen, err := binary.ReadUvarint(p.rw)
			if err != nil {
				return err
			}
			valueslot, valueoff, value := p.DW.NextValue(int(valuelen))
			if _, err := io.ReadFull(p.rw, value); err != nil {
				return err
			}
			n.ValueSlot = int32(valueslot)
			n.ValueOff = int32(valueoff)
			n.ValueLen = int32(valuelen)
			p.DW.AppendOplog(n)
		default:
			return ErrProto
		}
	}
}

type Delta struct {
	sub      sync.Map
	snapshot unsafe.Pointer
	publ     sync.Mutex
	sw       *SnapshotWriter
	tw       TreeWriter
	usage    *Usage
	head     Commit
}

func (d *Delta) setSnapshot(s *Snapshot) {
	atomic.StorePointer(&d.snapshot, unsafe.Pointer(s))
}

func (d *Delta) updateSnapshot() {
	d.setSnapshot(&Snapshot{
		Slots:   d.sw.Slots,
		HeadOff: int(d.head.Off),
	})
}

func (d *Delta) Snapshot() *Snapshot {
	return (*Snapshot)(atomic.LoadPointer(&d.snapshot))
}

func (d *Delta) Commit(diff *DiffTreeWriter, version int, checkversion bool) bool {
	d.publ.Lock()
	defer d.publ.Unlock()

	if checkversion {
		if int(d.head.Version+1) != version {
			return false
		}
	}

	c := Commit{
		Flags:   TagCommitFull | TagCommitParent | TagCommitDiff,
		Time:    time.Now().Unix(),
		Version: d.head.Version + 1,
	}
	c.DiffTree = int32(diff.WriteTree(&d.tw, d.sw))
	c.DiffUsageOff = int32(d.sw.WriteVarintArray(diff.Usage.Usage))
	d.usage.Add(diff.Usage)
	tree := MergeTree(d.sw.Slots, int(d.head.FullTree), int(c.DiffTree), d.usage, &d.tw, d.sw)
	c.FullTree = int32(tree)
	c.FullUsageOff = int32(d.sw.WriteVarintArray(d.usage.Usage))
	c.Parent = int32(d.head.Off)
	c.SlotsLenOff = int32(d.sw.WriteVarintArray(d.sw.SlotsLen))
	c.Off = int32(c.Write(d.sw))
	d.head = c

	d.updateSnapshot()
	d.notifyAll()

	return true
}

func (d *Delta) Compact() {
	d.publ.Lock()
	defer d.publ.Unlock()

	history := ReadHistory(d.sw.Slots, int(d.head.Off), 4)
	d.sw, d.head, d.usage = Compact(d.sw.Slots, history, &d.tw)
	d.updateSnapshot()
}

func (d *Delta) compactLoop() {
	for {
		d.Compact()
		time.Sleep(time.Second * 3)
	}
}

func (d *Delta) Restart(full *DiffTreeWriter, newversion int, checkversion bool) bool {
	d.publ.Lock()
	defer d.publ.Unlock()

	if checkversion {
		if newversion <= int(d.head.Version) {
			return false
		}
	}

	d.sw = NewSnapshotWriter(0)
	tree := full.WriteTree(&d.tw, d.sw)

	d.usage.Reset()
	d.usage.Add(full.Usage)

	c := Commit{
		Flags:   TagCommitFull,
		Time:    time.Now().Unix(),
		Version: int32(newversion),
	}
	c.FullTree = int32(tree)
	c.FullUsageOff = int32(d.sw.WriteVarintArray(d.usage.Usage))
	c.SlotsLenOff = int32(d.sw.WriteVarintArray(d.sw.SlotsLen))
	c.Off = int32(c.Write(d.sw))
	d.head = c

	d.updateSnapshot()
	d.notifyAll()

	return true
}

func (d *Delta) Subscribe() (chan struct{}, func()) {
	notify := make(chan struct{}, 1)
	unsub := func() {
		d.sub.Delete(notify)
	}
	d.sub.Store(notify, nil)
	return notify, unsub
}

func (d *Delta) notifyAll() {
	d.sub.Range(func(k, _ interface{}) bool {
		notify := k.(chan struct{})
		select {
		case notify <- struct{}{}:
		default:
		}
		return true
	})
}

func syncVersion(p *SyncProto, s *Snapshot, oldversion int) (int, error) {
	c := ReadCommit(s.Slots, s.HeadOff)

	newversion := int(c.Version)
	diffversion := newversion - oldversion

	if diffversion <= 0 {
		return newversion, nil
	}

	history := ReadHistory(s.Slots, s.HeadOff, diffversion)

	if len(history) < diffversion {
		if err := p.WriteTreeStart(SPTypeFull); err != nil {
			return -1, err
		}
		if err := p.WriteTreeVersion(newversion); err != nil {
			return -1, err
		}
		if err := p.WriteTree(s.Slots, int(c.FullTree)); err != nil {
			return -1, err
		}
		if err := p.WriteTreeEnd(); err != nil {
			return -1, err
		}
		return newversion, nil
	} else {
		for i := len(history) - 1; i >= 0; i-- {
			c := history[i]
			if err := p.WriteTreeStart(SPTypeDiff); err != nil {
				return -1, err
			}
			if err := p.WriteTree(s.Slots, int(c.DiffTree)); err != nil {
				return -1, err
			}
			if err := p.WriteTreeEnd(); err != nil {
				return -1, err
			}
		}
		return newversion, nil
	}
}

func (d *Delta) RequestSubscribeConn(rw0 io.ReadWriter) error {
	rw := bufio.NewReadWriter(
		bufio.NewReaderSize(rw0, 128),
		bufio.NewWriterSize(rw0, 128),
	)
	s := d.Snapshot()
	c := ReadCommit(s.Slots, s.HeadOff)

	p := NewSyncProto(rw)
	p.Role = SPRoleSubscribe
	p.Version = int(c.Version)

	version := int(c.Version)

	if err := p.WriteHeader(); err != nil {
		return err
	}

	for {
		typ, err := p.ReadTreeStart()
		if err != nil {
			return err
		}
		switch typ {
		case SPTypeDiff:
			if err := p.ReadTree(); err != nil {
				return err
			}
			version++
			d.Commit(&p.DW, version, true)

		case SPTypeFull:
			if err := p.ReadTree(); err != nil {
				return err
			}
			newversion, err := p.ReadTreeVersion()
			if err != nil {
				return err
			}
			d.Restart(&p.DW, newversion, true)
			version = newversion

		default:
			return ErrProto
		}
	}
}

func (d *Delta) HandleConn(rw0 io.ReadWriter) error {
	rw := bufio.NewReadWriter(
		bufio.NewReaderSize(rw0, 128),
		bufio.NewWriterSize(rw0, 128),
	)
	p := NewSyncProto(rw)

	if err := p.ReadHeader(); err != nil {
		return err
	}

	switch p.Role {
	case SPRolePulish:
		for {
			typ, err := p.ReadTreeStart()
			if err != nil {
				return err
			}
			switch typ {
			case SPTypeDiff:
				if err := p.ReadTree(); err != nil {
					return err
				}
				ok := d.Commit(&p.DW, -1, false)
				if debugDelta {
					fmt.Println("publish", ok)
				}

			default:
				return ErrProto
			}
		}

	case SPRoleSubscribe:
		notify, unsub := d.Subscribe()
		defer unsub()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			rw.ReadByte()
			cancel()
		}()

		version := p.Version

		for {
			s := d.Snapshot()
			newversion, err := syncVersion(p, s, version)
			if err != nil {
				return err
			}
			version = newversion

			select {
			case <-ctx.Done():
				return nil
			case <-notify:
			}
		}

	default:
		return ErrProto
	}
}

func NewDelta() *Delta {
	sw := NewSnapshotWriter(0)
	tree := WriteEmptyTree(sw)
	c0 := Commit{
		Flags:   TagCommitFull,
		Time:    time.Now().Unix(),
		Version: 1,
	}
	usage := &Usage{}
	c0.FullTree = int32(tree)
	c0.FullUsageOff = int32(sw.WriteVarintArray(usage.Usage))
	c0.Off = int32(c0.Write(sw))
	d := &Delta{
		sw:    sw,
		head:  c0,
		usage: usage,
	}
	d.updateSnapshot()
	return d
}
