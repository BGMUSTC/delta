package delta

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	OpSet    = 1
	OpRemove = 2

	TagCommitParent = 1 << 0

	TagNodeKey   = 1 << 0
	TagNodeValue = 1 << 1
	TagNodeRef   = 1 << 2
	TagNodeChild = 1 << 3
	tagNodeMerge = 1 << 4 // inner use

	TagPublishCommit = 1

	TagPublishCommitOplog = 1

	TagPublishCommitOplogOp    = 1
	TagPublishCommitOplogKey   = 2
	TagPublishCommitOplogValue = 3
)

const (
	debugBuildCommitTree = false
	debugMergeTree       = false
)

var ErrProtoRead = fmt.Errorf("proto read failed")
var ErrInvalidCommit = fmt.Errorf("invalid commit")
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
		panic(ErrProtoRead)
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
					fmt.Println(" value", string(value))
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
			tw.treeb.WriteByte(byte(n.ChildNr - 1))

			for ci := n.ChildStart; ci < n.ChildStart+int32(n.ChildNr); ci++ {
				c := tw.Nodes[ci]
				ch := slots[0][int(c.KeyOff)]
				tw.treeb.WriteByte(ch)
				off := int(c.Off)
				if c.OffIsAbs {
					off = -(w.Slot0Len - off)
				}
				ptr := tw.treeb.Len() - off
				putInt(tw.treeb.NextBytes(ptrsize), ptr, ptrsize)
				if debugBuildCommitTree {
					fmt.Println(" index", "ch", chstr(int(ch)), "idx", ci, "ptr", off)
				}
			}
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

func readNodeKeyOff(t []byte, off int) int32 {
	r := ProtoReader{B: t, Off: off}
	flags := r.ReadU8()

	if flags&TagNodeKey != 0 {
		r.ReadUvarint()
		return int32(r.Off)
	}

	panic("invalid")
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

func mergeTwo(slots [][]byte, n0, n1 Node, used *Used, tw *TreeWriter) Node {
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
					used.Sub(n0.ValueSlot, n0.ValueLen)
					if n1.ValueOp == OpSet {
						used.Add(n1.ValueSlot, n1.ValueLen)
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

func MergeTree(slots [][]byte, tree0, tree1 int, used *Used, tw *TreeWriter) {
	t := slots[0]

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
			tw.Nodes[ni] = mergeTwo(slots, n0, n1, used, tw)
		}
	}
}

// type TreeBfs struct {
// }

// func (s *TreeBfs) Search(slots [][]byte, off int, tw *TreeWriter) {
// 	t := slots[0]
// 	n := ReadNode(t, off)

// 	cr := InitChildReader(t, int(n.ChildStart))
// 	for {
// 		ch, ptr := cr.Next()
// 		if ch == -1 {
// 			break
// 		}
// 	}
// }

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
			return true, n.Value(slots)
		}
	}
	return false, nil
}

type rangeDfs struct {
	slots  [][]byte
	prefix []byte
	fn     func(k, v []byte)
	n      Node
}

func (d *rangeDfs) search() {
	key := d.n.Key(d.slots)
	d.prefix = append(d.prefix, key...)
	if d.n.ValueOp == OpSet {
		d.fn(d.prefix, d.n.Value(d.slots))
	}
	cr := InitChildReader(d.slots[0], int(d.n.ChildStart))
	for !cr.End() {
		_, ptr := cr.Next()
		d.n = ReadNode(d.slots[0], ptr)
		d.search()
	}
	d.prefix = d.prefix[:len(d.prefix)-len(key)]
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

	if n.KeyOff != 0 {
		debugPrintDepth(depth)
		fmt.Println("key", string(n.Key(slots)))
	}

	if n.ValueOp != 0 {
		debugPrintDepth(depth)
		value := slots[int(n.ValueSlot)][int(n.ValueOff):int(n.ValueOff+n.ValueLen)]
		fmt.Println("value", string(value), "op", n.ValueOp)
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

type Used struct {
	Used []int
}

func (u *Used) Reset() {
	u.Used = u.Used[:0]
}

func (u *Used) prepare(slot int) {
	for len(u.Used) < slot {
		u.Used = append(u.Used, 0)
	}
}

func (u *Used) AddValue(slot, vlen int) {
	u.prepare(slot)
	u.Used[slot] += vlen
}

func sumLen(a []int) int {
	n := 0
	for _, v := range a {
		n += v
	}
	return n
}

func Compact(slots [][]byte, head int, slotslen []int) {
	history := []Commit{}
	now := time.Now()
	for {
		c := ReadCommit(slots, head)
		ctime := time.Unix(c.Time, 0)
		if now.Sub(ctime) > time.Second {
			break
		}
		if len(history) > 128 {
			break
		}
		history = append(history, c)
		if !c.HasParent() {
			break
		}
	}
	if len(history) < 2 {
		return
	}

	totallen := sumLen(slotslen)
	totalused := sumLen(dfs.used)
	if float64(totalused)/float64(totallen) > 0.7 {
		return
	}
}

type Commit struct {
	Flags       byte
	Version     int32
	Tree        int32
	DiffTree    int32
	Parent      int32
	Time        int64
	UsedOff     int32
	DiffUsedOff int32
}

func (c Commit) HasParent() bool {
	return c.Flags&TagCommitParent != 0
}

func ReadCommit(slots [][]byte, off int) Commit {
	r := ProtoReader{B: slots[0], Off: off}
	c := Commit{}
	c.Flags = r.ReadU8()
	c.Version = int32(r.ReadUvarint())
	c.Tree = int32(r.ReadUvarint())
	c.DiffTree = int32(r.ReadUvarint())
	if c.Flags&TagCommitParent != 0 {
		off1 := int32(r.Off)
		parent0 := int32(r.ReadUvarint())
		c.Parent = parent0 - off1
	}
	c.Time = int64(r.ReadUvarint())
	c.UsedOff = int32(r.ReadUvarint())
	c.DiffUsedOff = int32(r.ReadUvarint())
	return c
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

var DefaultValueSlotSize = 1024 * 128
var DefaultTreeSlotSize = 1024 * 128

type SnapshotWriter struct {
	Slots    [][]byte
	SlotsLen []int
	Slot0Len int
	WSlot    int
	b        Buffer
}

func NewSnapshotWriter() *SnapshotWriter {
	w := &SnapshotWriter{
		Slots:    make([][]byte, 2, 16),
		SlotsLen: make([]int, 2, 16),
		WSlot:    1,
	}
	w.Slots[0] = make([]byte, DefaultTreeSlotSize)
	w.Slots[1] = make([]byte, DefaultValueSlotSize)
	w.Slot0Len = 0
	return w
}

func (w *SnapshotWriter) NextValue(n int) (int, int, []byte) {
	if n > DefaultValueSlotSize {
		slot := len(w.Slots)
		off := 0
		b := make([]byte, n)
		w.Slots = append(w.Slots, b)
		w.SlotsLen = append(w.SlotsLen, len(b))
		return slot, off, b
	}

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

func (w *SnapshotWriter) newSlots() {
	newslots := make([][]byte, len(w.Slots), cap(w.Slots))
	copy(newslots, w.Slots)
	w.Slots = newslots
}

func (w *SnapshotWriter) WriteTree(b []byte) {
	if n := int(w.Slot0Len) + len(b); n > len(w.Slots[0]) {
		w.newSlots()
		newslot0 := make([]byte, n*2)
		copy(newslot0, w.Slots[0])
		w.Slots[0] = newslot0
	}
	copy(w.Slots[0][int(w.Slot0Len):], b)
	w.Slot0Len += len(b)
}

func (w *SnapshotWriter) WriteVarintArray(a []int) int {
	off := w.Slot0Len
	w.b.Reset()
	writeUvarint(&w.b, len(a))
	for _, v := range a {
		writeUvarint(&w.b, v)
	}
	w.WriteTree(w.b.Bytes())
	return off
}

type Snapshot struct {
	Slots       [][]byte
	Slot0Len    int
	WSlot       int
	HeadOff     int
	SlotsLenOff int
	Version     int
}

type Delta struct {
	sub      sync.Map
	snapshot unsafe.Pointer
	publ     sync.Mutex
}

func (d *Delta) Subscribe() (chan struct{}, func()) {
	notify := make(chan struct{}, 1)
	remove := func() {
		d.sub.Delete(notify)
	}
	d.sub.Store(notify, nil)
	return notify, remove
}

func (d *Delta) notifyAll() {
}

func (d *Delta) RequestSubscribe(c *bufio.ReadWriter) {
}

type DiffTreeWriter struct {
	W        *SnapshotWriter
	Used     Used
	bkey     Buffer
	Oplogs   []Node
	Oplog    Node
	tmpslots [][]byte
}

func (c *DiffTreeWriter) Reset() {
	c.Used.Reset()
	c.Oplogs = c.Oplogs[:0]
}

func (c *DiffTreeWriter) WriteOplogSet(k, v []byte) {
	c.StartOplog()
	copy(c.NextKey(len(k)), k)
	copy(c.NextValue(len(v)), v)
	c.Oplog.ValueOp = OpSet
	c.EndOplog()
}

func (c *DiffTreeWriter) WriteOplogRemove(k []byte) {
	c.StartOplog()
	copy(c.NextKey(len(k)), k)
	c.Oplog.ValueOp = OpRemove
	c.EndOplog()
}

func (c *DiffTreeWriter) StartOplog() {
	c.Oplog = Node{
		Off: int32(len(c.Oplogs)),
	}
}

func (c *DiffTreeWriter) NextKey(n int) []byte {
	c.Oplog.KeyOff = int32(c.bkey.Len())
	c.Oplog.KeyLen = int32(n)
	key := c.bkey.NextBytes(n)
	return key
}

func (c *DiffTreeWriter) NextValue(n int) []byte {
	slot, off, b := c.W.NextValue(n)
	c.Oplog.ValueSlot = int32(slot)
	c.Oplog.ValueOff = int32(off)
	c.Oplog.ValueLen = int32(n)
	return b
}

func (c *DiffTreeWriter) EndOplog() {
	c.Oplogs = append(c.Oplogs, c.Oplog)
}

func (c *DiffTreeWriter) WriteTree(tw *TreeWriter) int {
	c.tmpslots = c.tmpslots[:0]
	c.tmpslots = append(c.tmpslots, c.bkey.Bytes())
	for i := 1; i < len(c.W.Slots); i++ {
		c.tmpslots = append(c.tmpslots, c.W.Slots[i])
	}
	defer func() {
		for i := 1; i < len(c.W.Slots); i++ {
			c.tmpslots[i] = nil
		}
	}()
	BuildCommitTree(tw, c.tmpslots, c.Oplogs)
	return tw.Write(c.tmpslots, c.W)
}

type deltaPublish struct {
	d  *Delta
	rw *bufio.ReadWriter
	tw DiffTreeWriter
}

func (p *deltaPublish) handleCommitOplog() error {
	for {
		tag, err := p.rw.ReadByte()
		if err != nil {
			return err
		}

		switch tag {
		case TagPublishCommitOplogOp:
			v, err := p.rw.ReadByte()
			if err != nil {
				return err
			}
			p.tw.Oplog.ValueOp = int32(v)

		case TagPublishCommitOplogKey:
			v, err := p.rw.ReadByte()
			if err != nil {
				return err
			}
			blen := int(v)
			key := p.tw.NextKey(blen)
			if _, err := io.ReadFull(p.rw, key); err != nil {
				return err
			}

		case TagPublishCommitOplogValue:
			v, err := p.rw.ReadByte()
			if err != nil {
				return err
			}
			blen := int(v)
			value := p.tw.NextValue(blen)
			if _, err := io.ReadFull(p.rw, value); err != nil {
				return err
			}

		case 0:
			return nil
		}
	}
}

func (p *deltaPublish) handleCommit() error {
	for {
		tag, err := p.rw.ReadByte()
		if err != nil {
			return err
		}

		switch tag {
		case TagPublishCommitOplog:
			if err := p.handleCommitOplog(); err != nil {
				return err
			}

		case 0:
			return nil
		}
	}
}

func (p *deltaPublish) handle() error {
	for {
		tag, err := p.rw.ReadByte()
		if err != nil {
			return err
		}
		switch tag {
		case TagPublishCommit:
			if err := p.handleCommit(); err != nil {
				return err
			}
		default:
			return ErrInvalidPublish
		}
	}
}

func (d *Delta) HandlePublish(rw io.ReadWriter) error {
	p := &deltaPublish{
		d: d,
		rw: bufio.NewReadWriter(
			bufio.NewReaderSize(rw, 128),
			bufio.NewWriterSize(rw, 128),
		),
	}
	return p.handle()
}
