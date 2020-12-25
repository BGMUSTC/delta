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
	"unsafe"
)

const (
	OpSet    = 1
	OpRemove = 2

	TagHeadVersion = 1
	TagHeadTree    = 2
	TagHeadHistory = 3

	TagHistoryCommit = 1

	TagNodePath  = 1 << 0
	TagNodeValue = 1 << 1
	TagNodeRef   = 1 << 2
	TagNodeChild = 1 << 3

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
	Off        int32
	Off1       int32
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

type TreeWriter struct {
	Nodes []Node
	treeb Buffer
}

func oplogKey(slots [][]byte, o Node) []byte {
	keys := slots[0]
	return keys[int(o.KeyOff):int(o.KeyOff+o.KeyLen)]
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
				n.ValueOp = o.ValueOp
				n.ValueSlot = o.ValueSlot
				n.ValueOff = o.ValueOff
				n.ValueLen = o.ValueLen
			} else {
				if debugBuildCommitTree {
					fmt.Println("cont", cont, "ch", string([]byte{byte(last)}))
				}
				if cont == oplogend-oplogstart {
					n.KeyLen++
					return true
				} else {
					tw.Nodes = append(tw.Nodes, Node{
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
	// ChildStart => OplogStart
	// ChildNr => OplogNr
	// KeyOff => LevelStart
	// KeyLen => LevelLen

	sortOplog(slots, oplogs)

	tw.Nodes = tw.Nodes[:0]
	tw.Nodes = append(tw.Nodes, Node{
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
			n.ValueOp = int32(o.ValueOp)
			n.ValueSlot = o.ValueSlot
			n.ValueOff = o.ValueOff
			n.ValueLen = o.ValueLen
			levelStart := n.KeyOff
			n.KeyOff = o.KeyOff + levelStart
			n.KeyLen = o.KeyLen - levelStart
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
		}
	}
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

func (tw *TreeWriter) Write(w *SnapshotRewriter) int {
	if debugBuildCommitTree {
		fmt.Println("write")
	}

	tw.treeb.Reset()

	for ni := len(tw.Nodes) - 1; ni >= 0; ni-- {
		n := &tw.Nodes[ni]

		if n.KeyLen == 0 && n.RefOff == 0 && n.ValueOp == 0 && n.ChildNr == 0 {
			continue
		}

		n.Off = int32(tw.treeb.Len())

		if debugBuildCommitTree {
			fmt.Println("node", ni, "off", n.Off, "keyoff", n.KeyOff, "keylen", n.KeyLen, "childstart", n.ChildStart, "childnr", n.ChildNr)
		}

		flags := byte(0)
		flagsoff := tw.treeb.Len()
		tw.treeb.NextBytes(1)

		// path
		if n.KeyLen > 0 {
			flags |= TagNodePath
			writeUvarint(&tw.treeb, int(n.KeyLen))
			path := w.OrigSlots[0][int(n.KeyOff):int(n.KeyOff+n.KeyLen)]
			tw.treeb.Write(path)
			if debugBuildCommitTree {
				fmt.Println(" path", string(path))
			}
		}

		// ref
		if n.RefOff != 0 {
			flags |= TagNodeRef
			writeUvarint(&tw.treeb, int(n.RefOff))
		}

		// value
		if n.ValueOp != 0 {
			flags |= TagNodeValue
			tw.treeb.WriteByte(byte(n.ValueOp))
			if debugBuildCommitTree {
				fmt.Println(" op", n.ValueOp)
			}
			if n.ValueOp == OpSet {
				slot, off := w.WriteValue(int(n.ValueSlot), int(n.ValueOff), int(n.ValueLen))
				writeUvarint(&tw.treeb, slot)            // slot
				writeUvarint(&tw.treeb, off)             // off
				writeUvarint(&tw.treeb, int(n.ValueLen)) // size
				if debugBuildCommitTree {
					value := w.OrigSlots[int(n.ValueSlot)][int(n.ValueOff):int(n.ValueOff+n.ValueLen)]
					fmt.Println(" value", string(value))
				}
			}
		}

		// child
		if n.ChildNr > 0 {
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
				ch := w.OrigSlots[0][int(c.KeyOff)]
				tw.treeb.WriteByte(ch)
				var off int
				if c.Off1 != 0 {
					off = -(w.Slot0Size - int(c.Off1))
				} else {
					off = int(c.Off)
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

	off := w.Slot0Size
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

	if flags&TagNodePath != 0 {
		r.ReadUvarint()
		return int32(r.Off)
	}

	panic("invalid")
}

func ReadNode(t []byte, off int) Node {
	n := Node{
		Off: int32(off),
	}

	r := ProtoReader{B: t, Off: off}
	flags := r.ReadU8()

	if flags&TagNodePath != 0 {
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

func mergeTwoNodeChild(slots [][]byte, n0, n1 Node, ni int, tw *TreeWriter) {
	cr0 := InitChildReader(slots[0], int(n0.ChildStart))
	cr1 := InitChildReader(slots[0], int(n1.ChildStart))

	for {
		if cr0.End() && cr1.End() {
			break
		} else if !cr0.End() && !cr1.End() {
			ch0, ptr0 := cr0.Peek()
			ch1, ptr1 := cr1.Peek()
			if debugMergeTree {
				fmt.Println(" merge child", "ch0", chstr(ch0), "ch1", chstr(ch1), "ptr0", ptr0, "ptr1", ptr1)
			}
			if ch0 < ch1 {
				tw.Nodes = append(tw.Nodes, Node{
					Off1:   int32(ptr0),
					KeyOff: readNodeKeyOff(slots[0], ptr0),
				})
				cr0.Next()
			} else if ch0 == ch1 {
				tw.Nodes = append(tw.Nodes, Node{
					Off:  int32(ptr0),
					Off1: int32(ptr1),
				})
				cr0.Next()
				cr1.Next()
			} else {
				tw.Nodes = append(tw.Nodes, Node{
					Off1:   int32(ptr1),
					KeyOff: readNodeKeyOff(slots[0], ptr1),
				})
				cr1.Next()
			}
		} else if !cr0.End() {
			ch0, ptr0 := cr0.Peek()
			if debugMergeTree {
				fmt.Println(" merge child", "ch0", chstr(ch0), "ptr0", ptr0)
			}
			tw.Nodes = append(tw.Nodes, Node{
				Off1:   int32(ptr0),
				KeyOff: readNodeKeyOff(slots[0], ptr0),
			})
			cr0.Next()
		} else {
			ch1, ptr1 := cr1.Peek()
			if debugMergeTree {
				fmt.Println(" merge child", "ch1", chstr(ch1), "ptr1", ptr1)
			}
			tw.Nodes = append(tw.Nodes, Node{
				Off1:   int32(ptr1),
				KeyOff: readNodeKeyOff(slots[0], ptr1),
			})
			cr1.Next()
		}
	}
}

func chstr(ch int) string {
	if ch == -1 {
		return "-1"
	}
	return string([]byte{byte(ch)})
}

func mergeTwoNode(slots [][]byte, n0, n1 Node, ni int, tw *TreeWriter) {
	if debugMergeTree {
		fmt.Println("merge node", n0.Off, n1.Off, "keyoff", n0.KeyOff, n1.KeyOff, "keylen", n0.KeyLen, n1.KeyLen)
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
			fmt.Println(" ki", ki, "ch0", chstr(ch0), "ch1", chstr(ch1))
		}

		if ch0 == -1 && ch1 == -1 {
			// both end
			// use n1's value
			if debugMergeTree {
				fmt.Println(" both end")
			}
			tw.Nodes[ni] = Node{
				KeyOff:     n0.KeyOff,
				KeyLen:     int32(ki),
				ChildStart: int32(len(tw.Nodes)),
				ValueOp:    n1.ValueOp,
				ValueSlot:  n1.ValueSlot,
				ValueOff:   n1.ValueOff,
				ValueLen:   n1.ValueLen,
			}
			// merge both child next
			mergeTwoNodeChild(slots, n0, n1, ni, tw)
			tw.Nodes[ni].ChildNr = int32(len(tw.Nodes) - int(tw.Nodes[ni].ChildStart))
			return

		} else if ch0 != -1 && ch1 != -1 {
			if ch0 != ch1 {
				// both not end
				// child is n0,n1
				if debugMergeTree {
					fmt.Println(" both not end")
				}
				tw.Nodes[ni] = Node{
					KeyOff:     n0.KeyOff,
					KeyLen:     int32(ki),
					ChildStart: int32(len(tw.Nodes)),
					ChildNr:    2,
				}
				tw.Nodes = append(tw.Nodes, Node{
					KeyOff: n0.KeyOff + int32(ki),
					KeyLen: n0.KeyLen - int32(ki),
					RefOff: n0.Off,
				})
				tw.Nodes = append(tw.Nodes, Node{
					KeyOff: n1.KeyOff + int32(ki),
					KeyLen: n1.KeyLen - int32(ki),
					RefOff: n1.Off,
				})
				return

			} else {
				// continue
			}
		} else if ch0 != -1 {
			// only n0 end
			// use n0's value, child is n1
			if debugMergeTree {
				fmt.Println(" only n0 end")
			}
			tw.Nodes[ni] = Node{
				KeyOff:     n0.KeyOff,
				KeyLen:     int32(ki),
				ChildStart: int32(len(tw.Nodes)),
				ChildNr:    1,
				ValueOp:    n0.ValueOp,
				ValueSlot:  n0.ValueSlot,
				ValueOff:   n0.ValueOff,
				ValueLen:   n0.ValueLen,
			}
			tw.Nodes = append(tw.Nodes, Node{
				KeyOff: n1.KeyOff + int32(ki),
				KeyLen: n1.KeyLen - int32(ki),
				RefOff: n1.Off,
			})
			return

		} else if ch1 != -1 {
			// only n1 end
			// use n1's value, child is n0
			if debugMergeTree {
				fmt.Println(" only n1 end")
			}
			tw.Nodes[ni] = Node{
				KeyOff:     n0.KeyOff,
				KeyLen:     int32(ki),
				ChildStart: int32(len(tw.Nodes)),
				ChildNr:    1,
				ValueOp:    n1.ValueOp,
				ValueSlot:  n1.ValueSlot,
				ValueOff:   n1.ValueOff,
				ValueLen:   n1.ValueLen,
			}
			tw.Nodes = append(tw.Nodes, Node{
				KeyOff: n0.KeyOff + int32(ki),
				KeyLen: n0.KeyLen - int32(ki),
				RefOff: n0.Off,
			})
			return

		}
		ki++
	}
}

func MergeTree(slots [][]byte, tree0, tree1 int, tw *TreeWriter, w *SnapshotRewriter) {
	t := slots[0]

	tw.Nodes = tw.Nodes[:0]

	tw.Nodes = append(tw.Nodes, Node{
		Off:  int32(tree0),
		Off1: int32(tree1),
	})

	for ni := 0; ni < len(tw.Nodes); ni++ {
		n := tw.Nodes[ni]

		if n.Off == 0 && n.Off1 == 0 {
			// ref
		} else if n.Off != 0 && n.Off1 != 0 {
			// merge two
			n0 := ReadNode(t, int(n.Off))
			n1 := ReadNode(t, int(n.Off1))
			mergeTwoNode(slots, n0, n1, ni, tw)
		} else {
			// only off
		}
	}
}

type TreeDfs struct {
	kb Buffer
}

type TreeDfsFunc func(k []byte, op int, value []byte) bool

func (d *TreeDfs) search(slots [][]byte, off int, fn TreeDfsFunc) {
	t := slots[0]
	n := ReadNode(t, off)

	if n.KeyOff != 0 {
		key := slots[0][int(n.KeyOff):int(n.KeyOff+n.KeyLen)]
		d.kb.Write(key)
		defer d.kb.Back(len(key))
	}

	var value []byte
	if n.ValueOp == OpSet {
		value = slots[n.ValueSlot][int(n.ValueOff):int(n.ValueOff+n.ValueLen)]
	}

	if fn(d.kb.Bytes(), int(n.ValueOp), value) {
		return
	}

	cr := InitChildReader(t, int(n.ChildStart))
	for {
		ch, off := cr.Next()
		if ch == -1 {
			break
		}
		d.search(slots, off, fn)
	}
}

func (d *TreeDfs) Search(slots [][]byte, off int, fn TreeDfsFunc) {
	d.search(slots, off, fn)
}

func Range(slots [][]byte, off int, prefix []byte, fn func(k, v []byte)) {
	d := &TreeDfs{}
	d.Search(slots, off, func(k []byte, op int, value []byte) bool {
		if len(k) < len(prefix) {
			return bytes.Compare(k, prefix[:len(k)]) != 0
		} else {
			if bytes.Compare(k[:len(prefix)], prefix) != 0 {
				return true
			}
			if op == OpSet {
				fn(k, value)
			}
			return false
		}
	})
}

func Get(slots [][]byte, off int) (bool, []byte) {
}

func debugPrintDepth(depth int) {
	fmt.Print(strings.Repeat(" ", depth*2))
}

func debugDfsTree(slots [][]byte, off int, depth int) {
	n := ReadNode(slots[0], off)

	debugPrintDepth(depth)
	fmt.Println("off", n.Off)

	if n.KeyOff != 0 {
		path := slots[0][int(n.KeyOff):int(n.KeyOff+n.KeyLen)]
		debugPrintDepth(depth)
		fmt.Println("path", string(path))
	}

	if n.ValueOp != 0 {
		debugPrintDepth(depth)
		value := slots[int(n.ValueSlot)][int(n.ValueOff):int(n.ValueOff+n.ValueLen)]
		fmt.Println("value", string(value), "op", n.ValueOp)
	}

	if n.ChildStart != 0 {
		cr := InitChildReader(slots[0], int(n.ChildStart))
		for {
			ch, ptr := cr.Next()
			if ch == -1 {
				break
			}
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

func DebugDfsTree2(slots [][]byte, off int) {
	d := &TreeDfs{}
	d.Search(slots, off, func(k []byte, op int, value []byte) bool {
		fmt.Println("k", string(k), "op", op, "value", string(value))
		return false
	})
}

var DefaultValueSlotSize = 1024 * 128
var DefaultTreeSlotSize = 1024 * 128

type SnapshotRewriter struct {
	OrigSlots [][]byte
	Slots     [][]byte
	Sizes     []int
	Slot0Size int
	SizesOff  int
	WSlot     int
	sizesb    Buffer
}

func NewSnapshotRewriter(origslots [][]byte) *SnapshotRewriter {
	w := &SnapshotRewriter{
		OrigSlots: origslots,
		Slots:     make([][]byte, 2, 16),
		Sizes:     make([]int, 2, 16),
		WSlot:     1,
	}
	w.Slots[0] = make([]byte, DefaultTreeSlotSize)
	w.Slots[1] = make([]byte, DefaultValueSlotSize)
	w.Sizes[0] = 1
	w.Sizes[1] = 1
	w.Slot0Size = 1
	return w
}

func (w *SnapshotRewriter) reserveValue(n int) {
	if n > DefaultValueSlotSize {
		panic("too big")
	}
	for {
		if w.Sizes[w.WSlot]+n <= len(w.Slots[w.WSlot]) {
			return
		}
		w.WSlot++
		if w.WSlot == len(w.Slots) {
			w.Slots = append(w.Slots, make([]byte, DefaultValueSlotSize))
			w.Sizes = append(w.Sizes, 0)
		}
	}
}

func (w *SnapshotRewriter) newSlots() {
	newslots := make([][]byte, len(w.Slots), cap(w.Slots))
	copy(newslots, w.Slots)
	w.Slots = newslots
}

func (w *SnapshotRewriter) WriteTree(b []byte) {
	if n := int(w.Slot0Size) + len(b); n > len(w.Slots[0]) {
		w.newSlots()
		newslot0 := make([]byte, n*2)
		copy(newslot0, w.Slots[0])
		w.Slots[0] = newslot0
	}
	copy(w.Slots[0][int(w.Slot0Size):], b)
	w.Slot0Size += len(b)
}

func (w *SnapshotRewriter) WriteValue(slot, off, size int) (int, int) {
	b := w.OrigSlots[slot][off : off+size]
	if slot > 1 {
		if len(b) > DefaultValueSlotSize {
			panic("too big")
		}
		w.reserveValue(len(b))
		slot := w.WSlot
		off := w.Sizes[w.WSlot]
		w.Sizes[w.WSlot] += len(b)
		copy(w.Slots[slot][off:], b)
		return slot, off
	} else {
		slot := len(w.Slots)
		off := 0
		w.Slots = append(w.Slots, b)
		w.Sizes = append(w.Sizes, len(b))
		return slot, off
	}
}

func (w *SnapshotRewriter) Finish() *Snapshot {
	w.SizesOff = w.Slot0Size
	w.sizesb.Reset()
	writeUvarint(&w.sizesb, len(w.Sizes))
	for _, v := range w.Sizes {
		writeUvarint(&w.sizesb, v)
	}
	w.WriteTree(w.sizesb.Bytes())
	return &Snapshot{
		Slots:     w.Slots,
		Slot0Size: int(w.Slot0Size),
		SizesOff:  int(w.SizesOff),
	}
}

type Snapshot struct {
	Slots     [][]byte
	Slot0Size int
	SizesOff  int
	WSlot     int
	Head      int
	Version   int
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

type Commit struct {
	Slots  [][]byte
	bkey   Buffer
	bvalue Buffer
	Oplogs []Node
	Oplog  Node
}

func (c *Commit) Reset() {
	for i := range c.Slots {
		c.Slots[i] = nil
	}
	c.bkey.Reset()
	c.bvalue.Reset()
	c.Slots = c.Slots[:0]
	c.Oplogs = c.Oplogs[:0]
}

func (c *Commit) Start() {
	c.Slots = append(c.Slots, nil)
	c.Slots = append(c.Slots, nil)
	c.bkey.NextBytes(1)
	c.bvalue.NextBytes(1)
}

func (c *Commit) End() {
	c.Slots[0] = c.bkey.Bytes()
	c.Slots[1] = c.bvalue.Bytes()
}

func (c *Commit) AddOplogSet(k, v []byte) {
	c.StartOplog()
	copy(c.NextKey(len(k)), k)
	copy(c.NextValue(len(v)), v)
	c.Oplog.ValueOp = OpSet
	c.EndOplog()
}

func (c *Commit) AddOplogRemove(k []byte) {
	c.StartOplog()
	copy(c.NextKey(len(k)), k)
	c.Oplog.ValueOp = OpRemove
	c.EndOplog()
}

func (c *Commit) StartOplog() {
	c.Oplog = Node{
		Off: int32(len(c.Oplogs)),
	}
}

func (c *Commit) NextKey(n int) []byte {
	c.Oplog.KeyOff = int32(c.bkey.Len())
	c.Oplog.KeyLen = int32(n)
	key := c.bkey.NextBytes(n)
	return key
}

func (c *Commit) NextValue(n int) []byte {
	var b []byte
	if n < 1024*32 {
		c.Oplog.ValueSlot = 1
		c.Oplog.ValueOff = int32(c.bvalue.Len())
		c.Oplog.ValueLen = int32(n)
		b = c.bvalue.NextBytes(n)
	} else {
		b = make([]byte, n)
		c.Oplog.ValueSlot = int32(len(c.Slots))
		c.Oplog.ValueOff = 0
		c.Oplog.ValueLen = int32(n)
		c.Slots = append(c.Slots, b)
	}
	return b
}

func (c *Commit) EndOplog() {
	c.Oplogs = append(c.Oplogs, c.Oplog)
}

type deltaPublish struct {
	d  *Delta
	rw *bufio.ReadWriter
	c  Commit
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
			p.c.Oplog.ValueOp = int32(v)

		case TagPublishCommitOplogKey:
			v, err := p.rw.ReadByte()
			if err != nil {
				return err
			}
			blen := int(v)
			key := p.c.NextKey(blen)
			if _, err := io.ReadFull(p.rw, key); err != nil {
				return err
			}

		case TagPublishCommitOplogValue:
			v, err := p.rw.ReadByte()
			if err != nil {
				return err
			}
			blen := int(v)
			value := p.c.NextValue(blen)
			if _, err := io.ReadFull(p.rw, value); err != nil {
				return err
			}

		case 0:
			return nil
		}
	}
}

func (p *deltaPublish) handleCommit() error {
	defer p.c.Reset()
	p.c.Start()

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
			p.c.End()
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

// func writeFull(c io.Writer, h *Head) error {
// 	return nil
// }

// func (d *Delta) HandleSubscribe(c *bufio.ReadWriter) error {
// 	notify, remove := d.Subscribe()
// 	defer remove()

// 	ver, err := binary.ReadVarint(c)
// 	if err != nil {
// 		return err
// 	}

// 	h := d.Head()

// 	if h.LatestThan(ver) {
// 		if h.CanDiff(ver) {
// 			err := h.Diff(ver, func(op int, k, v []byte) error {
// 				// return writeDelta(c, op, k, v)
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		} else {
// 			if err := writeFull(c, h); err != nil {
// 				return err
// 			}
// 		}
// 	}

// 	for {
// 		select {
// 		case <-notify:
// 		}
// 	}
// }

// func allocInts(psrc *[]int, n int) []int {
// 	src := *psrc
// 	start := len(src)
// 	end := len(src) + n
// 	if end < cap(src) {
// 		src = src[:end]
// 	} else {
// 		new := make([]int, end, end*2)
// 		copy(new, src)
// 		src = new
// 	}
// 	*psrc = src
// 	b := src[start:end]
// 	return b
// }

// func ProtoReadNoPanic(fn func()) error {
// 	if err := recover(); err != nil {
// 		if err == ErrProtoRead {
// 			return ErrProtoRead
// 		}
// 		panic(err)
// 	}
// 	fn()
// 	return nil
// }
