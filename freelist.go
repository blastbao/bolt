package bolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// 当发起一个读事务时，Tx 单独复制一份 meta 信息，从这份独有的 meta 作为入口，可以读出该 meta 指向的数据，
// 此时即使有一个写事务修改了相关 key 的数据，新修改的数据只会被写入新的 page ，读事务持有的 page 会进入 pending 池，
// 因此该读事务相关的数据并不会被修改。只有该 page 相关的读事务都结束时，才会从 pending 池进入到 cache 池中，从而被复用修改。

// 当写事务更新数据时，并不直接覆盖老数据，而且分配一个新的 page 将更新后的数据写入，
// 然后将老数据占用的 page 放入 pending 池，建立新的索引。
// 当事务需要回滚时，只需要将 pending 池中的 page 释放，将索引回滚即完成数据的回滚。
// 这样加速了事务的回滚，减少了事务缓存的内存使用，同时避免了对正在读的事务的干扰。

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.

// 字段说明:
// 	1. ids 记录的是空闲可用的 page 的 pgid
//	2. pending 记录的是每个写事务释放的 page 的 pgid
//	3. cache 中记录的也是 ids 中的 pgid ，采用 map 为了方便查找

type freelist struct {
	ids     []pgid          // all free and available free page ids.
	pending map[txid][]pgid // mapping of soon-to-be free page ids by tx.
	cache   map[pgid]bool   // fast lookup of all free and pending page ids.
}


// newFreelist returns an empty, initialized freelist.
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

// size returns the size of the page after serialization.
// 返回 freelist 序列化后页面的大小。
func (f *freelist) size() int {

	// 获取当前 freelist 中所包含的 pages 数目
	n := f.count()

	// 如果 n 大于等于 uint16 类型最大值，则 pageHeader.count 无法容纳元素数，需要额外开辟一个 uint64 字节来存储，所以后续是 n+1 个 unsafe.Sizeof(pgid)
	if n >= 0xFFFF {
		// The first element will be used to store the count.
		// See freelist.write.
		n++
	}

	// header + data
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n)
}

// count returns count of pages on the freelist
func (f *freelist) count() int {

	//
	return f.free_count() + f.pending_count()
}



// free_count returns count of free pages
func (f *freelist) free_count() int {
	// freelist 中空闲可用的 page 的数目
	return len(f.ids)
}




// pending_count returns count of pending pages
func (f *freelist) pending_count() int {
	// freelist 中被写事务释放，但是可能被读事务占用的 page 总数，这部分 pages 可以被回收再利用
	var count int
	for _, list := range f.pending {
		count += len(list)
	}
	return count
}





// copyall copies into dst a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	mergepgids(dst, f.ids, m)
}





// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.

// 遍历 f.ids , 从中挑选出连续 n 个空闲的 page ，然后将其从 f.ids/f.cache 中剔除，然后返回起始的 page-id。
func (f *freelist) allocate(n int) pgid {

	// 如果没有可用 page 返回 0
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid

	// 遍历 ids , 从中挑选出连续 n 个空闲的 page ，然后将其从缓存中剔除，然后返回起始的 page-id
	for i, id := range f.ids {

		// 因为 DB 文件的起始 2 个 page 固定为 meta page ，因此有效的 page-id 不可能 <= 1 。
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// Reset initial page if this is not contiguous.
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// If we found a contiguous block then remove it and return it.

		if (id-initial)+1 == pgid(n) {

			// If we're allocating off the beginning then take the fast path and just adjust the existing slice.
			// This will use extra memory temporarily but the append() in free() will realloc the slice as is necessary.

			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// Remove from the free cache.
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}

			return initial
		}

		previd = id
	}

	// 不存在满足需求的 page ，返回 0
	return 0
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
//
// 当某个写事务产生无用 page 时，将调用 freelist.free(txid txid, p *page) 将指定 page p 放入 f.pending[txid] 池和 f.cache 中。
// 当下一个写事务开启时，会将没有被读事务引用的 pending 中的 page 搬移到 ids 缓存中，实现 page 的回收再利用。
// 之所以这样做，是为了支持事务的回滚和并发读事务，从而实现 MVCC 。

func (f *freelist) free(txid txid, p *page) {

	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// 数组 f.pending[txid] 中存储着写事务 txid 释放的 pages 的 pgid
	var ids = f.pending[txid]

	// Free page and all its overflow pages.
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {

		// Verify that page is not already free.
		// 验证是否是重复添加到 pending 中，panic!
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}

		// Add to the freelist and cache.
		// 将 id 页添加到 pending 数组中
		ids = append(ids, id)

		// 将 id 记录到 page cache 中
		f.cache[id] = true
	}

	// 重置 pending 数组
	f.pending[txid] = ids
}





// release moves all page ids for a transaction id (or older) to the freelist.
// release 将事务 txid (或比 txid 小的) 的 pending pages ids 移动到 f.ids 中。
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	// f.pending 中存储了写事务 tid 替换掉的 page ids，为了防止读事务失效将这些页面缓存到 pending 中，暂不释放和重用。
	// 由于小于 txid 的读事务都已经完成，因此把小于 txid 的写事务所替换的 pages 释放掉以支持重用不会影响正在进行的读事务。
	for tid, ids := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}
	// 先排序，因为后面 merge 函数的输入要求是有序数组
	sort.Sort(m)
	// 把释放掉的 page ids 移动到空闲页面列表 f.ids 中
	f.ids = pgids(f.ids).merge(m)
}





// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid txid) {



	// Remove page ids from cache.
	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}



	// Remove pages from pending list.
	delete(f.pending, txid)



}

// freed returns whether a given page is in the free list.
func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *page) {

	// 注意，p.count 是 uint16 类型的，最大值就是 64k，但如果 p 所包含元素数超过 64k，则 p.count 不足以容纳，
	// 此时 p.count 会被置为 0xffff，然后真实的元素数目会用一个 uint64 类型数值，保存在 p.ptr 指向的数据区的首部 8 bytes 中。
	idx, count := 0, int(p.count)

	// 如果 page.count 取 uint16 类型的最大值（0xFFFF = 64k），则视其为溢出，
	// 此时 freelist 的大小为 p.ptr 指向的数据区开始的第一个 uint64 数值。
	// 此时要设置 idx = 1 来跳过这个 uint64 ，以便于获取后续的 page id list 。
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0])
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.ids = nil
	} else {

		// 从 p.ptr 指向的数据区取出 page id list， 这里 idx 可能取值 0 或者 1 。
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		// Make sure they're sorted.
		// 对 page id list 从小到大进行排序。
		sort.Sort(pgids(f.ids))
	}

	// Rebuild the page cache.
	//
	f.reindex()
}



// write writes the page ids onto a freelist page.
// All free and pending ids are saved to disk since in the event of a program crash,
// all pending ids will become free.


func (f *freelist) write(p *page) error {

	// Combine the old free pgids and pgids waiting on an open transaction.
	//


	// Update the header flag.
	// 更新 freelist 页标识
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements so if we overflow that number then we handle it by putting the size in the first element.
	// 页头 page.count 最多只能容纳 64k 个元素，因此，如果真实数目溢出该范围，则会将 size 放在数据区域首个 uint64 字段中。
	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		p.count = uint16(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:])
	} else {

		// 注意，如果 p 所包含元素数超过 64k，则 uint16 类型的 p.count 不足以容纳，
		// 此时将 p.count 置为 0xffff，然后真实元素数目会用一个 uint64 类型数值，
		// 保存在 p.ptr 指向的数据区的首部 8 bytes 中，而 page id list 尾随其后来存储。
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:])
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
func (f *freelist) reload(p *page) {

	// 从 page p 中加载 freelist，会设置 f.ids
	f.read(p)

	// Build a cache of only pending pages.
	// 所有 pending 状态的 pages 都设置其 cache 标识为 true 。
	pcache := make(map[pgid]bool)
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist with any pages not in the pending lists.
	// 检查空闲列表 f.ids 中的每个页面 ...

	var a []pgid
	for _, id := range f.ids {
		if !pcache[id] {
			a = append(a, id)
		}
	}
	f.ids = a

	// Once the available list is rebuilt then rebuild the free cache so that
	// it includes the available and pending free pages.
	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {

	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}

	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}

}
