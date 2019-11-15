package bolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const minKeysPerPage = 2

const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))


// 四种页面类型
const (
	branchPageFlag   = 0x01 // 分支节点，对应 B+ tree 中的内节点
	leafPageFlag     = 0x02 // 叶子节点，对应 B+ tree 中的叶子节点
	metaPageFlag     = 0x04 // meta 页
	freelistPageFlag = 0x10 // freelist 页
)

const (
	bucketLeafFlag = 0x01
)

// page id 类型
type pgid uint64

// boltdb 采用了分页(page)的方式访问文件，默认情况下一个 page 的大小为 4KB(4096 bytes) ，每当从文件中读取和写入都是以 page 作为最小的基本单位。



// page 是 boltdb 持久化时，与磁盘相关的数据结构。 page 的大小采用操作系统内存页的大小，即 getpagesize 系统调用的返回值。
//
// 字段说明:
//
// 1. id 为 page 的序号，
// 1. flags 表示 page 的类型，有 branchPageFlag / leafPageFlag / metaPageFlag / freelistPageFlag 几种。
// 1. count
// 	当 page 是 freelistPageFlag 类型时，存储的是 freelist 中 pgid 数组中元素的个数;
// 	当 page 是其他类型时，存储的是 inode 的个数。
// 1. overflow 记录 page 中数据量超过一个 page 所能存储大小的时候需要额外的 page 的数目。
//
//
// 每个 page 对应对应一个磁盘上的数据块。这个数据块的 layout 为:
// | page struct data | page element items | k-v pairs |














//───────────────────────────────┬──────┬──────┬──────────────┬──────────────────────────────
//                               │      │      │              │
//──────────8 bytes──────────────┼──2───├─ 2───├─────4────────┼─────────────8────────────────
//                               │      │      │              │
//┌──────────────────────────────┬──────┬──────┬──────────────┬ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//│                              │      │      │              │                              │
//│           page id            │ flag │count │   overflow   │             ptr
//│                              │      │      │              │                              │
//└──────────────────────────────┴──────┴──────┴──────────────┼───────────────────────────────
//
// 一个 page 页面 = page header（没有ptr） + elements
type page struct {

	// 1. Page Header 部分
	id       pgid    // PageID，如 0,1,2。PageID 是用于从数据库文件的内存映射(mmap)中读取具体一页的索引值
	flags    uint16  // 表示 Page 存储的类型，包含 branchPageFlag、leafPageFlag 等四种




	count    uint16  // 表示 Page 存储的数据元素个数，包括 branchPage 和 leafPage 类型的页面中有用。对应的元素分别是 branchPageElement 和 leafPageElement 。
	overflow uint32  // 表示当前 Page 是否有后续 Page；如果有，表示后续页的数量，如果没有，则为0。


	// 2. Page Data 部分
	ptr      uintptr // ptr 用于标记页头 Page Header 部分结尾处，或者页面内存储数据 Page Data 部分的起始处。




	// page.ptr 保存页数据区域的起始地址，不同类型 page 保存的数据格式也不同，共有 4 种 page, 通过 flags 区分:
	//
	//	1. meta page: 		存放 db 的 meta data。
	//	2. freelist page: 	存放 db 的空闲 page。
	//	3. branch page: 	存放 branch node 的数据。
	//	4. leaf page: 		存放 leaf node 的数据


}

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}


// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// leafPageElement retrieves the leaf node by index
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }



// branchPageElement represents a node on a branch page.
type branchPageElement struct {
	// element 对应的 K/V 对的存储位置相对于当前 element 的偏移
	pos   uint32
	// element 对应的 Key 的长度，以字节为单位
	ksize uint32
	// element 指向的子节点所在 page 的页号
	pgid  pgid
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// leafPageElement represents a node on a leaf page.
type leafPageElement struct {
	// 标明当前 element 是否代表一个 Bucket ，如果是 Bucket 则其值为 1 ，如果不是则其值为 0 。
	flags uint32

	pos   uint32
	ksize uint32
	vsize uint32
}

// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}
