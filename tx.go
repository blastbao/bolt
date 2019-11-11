package bolt

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// txid represents the internal transaction identifier.
type txid uint64

// Tx represents a read-only or read/write transaction on the database.
// Read-only  transactions can be used for retrieving values for keys and creating cursors.
// Read/write transactions can create and remove buckets and create and remove keys.
//
// IMPORTANT: You must commit or rollback transactions when you are done with them.
// Pages can not be reclaimed by the writer until no more transactions are using them.
// A long running read transaction can cause the database to quickly grow.
type Tx struct {
	writable       bool				// 当前 tx 是否可写

	// managed 表示当前 transaction 是否被 db 托管，即通过 db.Update() 或者 db.View() 来写或者读数据库;
	// BoltDB 还支持直接调用 Tx 的相关方法进行读写，这时 managed 字段为false。
	managed        bool				// 当前 tx 是否通过 db.Update() 或者 db.View() 进行数据库操作

	db             *DB				// 当前 tx 归属的 db 对象
	meta           *meta			// 当前 tx 归属的 db 的 meta 信息
	root           Bucket			// 指根 bucket，所有 tx 都要从根开始查起
	pages          map[pgid]*page 	// 当前 tx 操作（读或写）的 page ，也被称作 dirty pages
	stats          TxStats			// 当前 tx 的操作统计
	commitHandlers []func() 		// 保存 tx 在 commit 时执行的回调函数


	// WriteFlag specifies the flag for write-related methods like WriteTo().
	// Tx opens the database file with the specified flag to copy the data.
	//
	// By default, the flag is unset, which works well for mostly in-memory workloads.
	// For databases that are much larger than available RAM,
	// set the flag to syscall.O_DIRECT to avoid trashing the page cache.

	// WriteFlag 指定复制或移动数据库文件时，文件的打开模式，Tx 在打开数据库时会根据该 Flag 来复制数据。
	// 默认情况 WriteFlag 是不设置的，对于内存操作则无所谓，但是当文件很大的时候，可以通过设置 syscall.O_DIRECT 避免 cache 抖动。
	WriteFlag int
}


// 在 Tx 中的每个操作都会将变化后的 B-Tree 上的 node 缓存到 Tx 的 Bucket 副本中，这个变化只对该 Tx 可见。
// 当有删除操作时，Tx 会将要释放的 page-id 暂存在 freelist 的 pending 池中，当 Tx 调用 Commit 时，
// freelist 会将 pending 的 page-id 真正标记为 free 状态，如果 Tx 调用 Rollback 则会将 pending 池中的 page-id 移除，
// 则使 Tx 的删除操作回滚。


// init initializes the transaction.
func (tx *Tx) init(db *DB) {

	// 1. 初始化
	tx.db = db		// 将 tx.db 初始化为传入的 db
	tx.pages = nil	// 将脏页字典 tx.pages 初始化为空

	// 2. 复制 meta 页

	// 每当一个 Tx 被创建时，会复制一份当前最新的 db.meta 到 tx.meta 中，这是因为 meta 可能被 Tx 修改（如 meta.txid 字段），
	// 因此在提交前，Tx 只会操作自己复制的这份 tx.meta 。

	tx.meta = &meta{}		 // 创建一个空的 meta 对象，用它初始化 tx.meta，用于存储 db.meta()
	db.meta().copy(tx.meta)  // 将 db.meta() 返回的数据库 meta 信息复制到刚创建的 tx.meta 对象中，这里是对象拷贝而不是指针拷贝


	// 3. 创建根 Bucket
	tx.root = newBucket(tx)			// 用 newBucket(tx *Tx) 创建一个 Bucket ，并将其设为根 Bucket
	tx.root.bucket = &bucket{}		// 创建匿名嵌套于 Bucket 中的 bucket ，用于备份存储 tx.meta.root
	*tx.root.bucket = tx.meta.root	// 存储 tx.meta.root 的目的是为找到 root bucket 所在页，进而从页中读出 root bucket



	// Increment the transaction id and add a page cache for writable transactions.
	//
	// 如果是可写的 transaction ，就将 meta 中的 txid 加 1 ；当可写 transaction commit 后，meta 就会更新到数据库文件中，
	// 数据库的修改版本号就增加了。可见读事务不增加 txid ，仅读写事务增加。
	if tx.writable {
		tx.pages = make(map[pgid]*page) // 初始化存储脏页的 map
		tx.meta.txid += txid(1)			// 事务ID txid++
	}

}

// ID returns the transaction id.
func (tx *Tx) ID() int {
	return int(tx.meta.txid)
}

// DB returns a reference to the database that created the transaction.
func (tx *Tx) DB() *DB {
	return tx.db
}

// Size returns current database size in bytes as seen by this transaction.
func (tx *Tx) Size() int64 {
	return int64(tx.meta.pgid) * int64(tx.db.pageSize)
}

// Writable returns whether the transaction can perform write operations.
func (tx *Tx) Writable() bool {
	return tx.writable
}

// Cursor creates a cursor associated with the root bucket.
// All items in the cursor will return a nil value because all root bucket keys point to buckets.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

// Stats retrieves a copy of the current transaction statistics.
func (tx *Tx) Stats() TxStats {
	return tx.stats
}

// Bucket retrieves a bucket by name.
// Returns nil if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) Bucket(name []byte) *Bucket {
	return tx.root.Bucket(name)
}

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	return tx.root.CreateBucket(name)
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return tx.root.CreateBucketIfNotExists(name)
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
func (tx *Tx) DeleteBucket(name []byte) error {
	return tx.root.DeleteBucket(name)
}

// ForEach executes a function for each bucket in the root.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller.
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.root.ForEach(func(k, v []byte) error {
		if err := fn(k, tx.root.Bucket(k)); err != nil {
			return err
		}
		return nil
	})
}

// OnCommit adds a handler function to be executed after the transaction successfully commits.
func (tx *Tx) OnCommit(fn func()) {
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

// Commit writes all changes to disk and updates the meta page.
// Returns an error if a disk write error occurs, or if Commit is
// called on a read-only transaction.
func (tx *Tx) Commit() error {

	_assert(!tx.managed, "managed tx commit not allowed")
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}

	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// Rebalance nodes which have had deletions.
	var startTime = time.Now()

	//
	tx.root.rebalance()
	if tx.stats.Rebalance > 0 {
		tx.stats.RebalanceTime += time.Since(startTime)
	}

	// spill data onto dirty pages.
	startTime = time.Now()
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)

	// Free the old root bucket.
	tx.meta.root.root = tx.root.root

	opgid := tx.meta.pgid

	// Free the freelist and allocate new pages for it. This will overestimate
	// the size of the freelist but not underestimate the size (which would be bad).
	tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist))
	p, err := tx.allocate((tx.db.freelist.size() / tx.db.pageSize) + 1)
	if err != nil {
		tx.rollback()
		return err
	}
	if err := tx.db.freelist.write(p); err != nil {
		tx.rollback()
		return err
	}
	tx.meta.freelist = p.id

	// If the high water mark has moved up then attempt to grow the database.
	if tx.meta.pgid > opgid {
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	// Write dirty pages to disk.
	startTime = time.Now()
	if err := tx.write(); err != nil {
		tx.rollback()
		return err
	}

	// If strict mode is enabled then perform a consistency check.
	// Only the first consistency error is reported in the panic.
	if tx.db.StrictMode {
		ch := tx.Check()
		var errs []string
		for {
			err, ok := <-ch
			if !ok {
				break
			}
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			panic("check fail: " + strings.Join(errs, "\n"))
		}
	}

	// Write meta to disk.
	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime)

	// Finalize the transaction.
	tx.close()

	// Execute commit handlers now that the locks have been removed.
	for _, fn := range tx.commitHandlers {
		fn()
	}

	return nil
}

// Rollback closes the transaction and ignores all previous updates. Read-only
// transactions must be rolled back and not committed.
func (tx *Tx) Rollback() error {
	_assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.rollback()
	return nil
}

func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.freelist.rollback(tx.meta.txid)
		tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist))
	}
	tx.close()
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		// Grab freelist stats.
		var freelistFreeN = tx.db.freelist.free_count()
		var freelistPendingN = tx.db.freelist.pending_count()
		var freelistAlloc = tx.db.freelist.size()

		// Remove transaction ref & writer lock.
		tx.db.rwtx = nil
		tx.db.rwlock.Unlock()

		// Merge statistics.
		tx.db.statlock.Lock()
		tx.db.stats.FreePageN = freelistFreeN
		tx.db.stats.PendingPageN = freelistPendingN
		tx.db.stats.FreeAlloc = (freelistFreeN + freelistPendingN) * tx.db.pageSize
		tx.db.stats.FreelistInuse = freelistAlloc
		tx.db.stats.TxStats.add(&tx.stats)
		tx.db.statlock.Unlock()
	} else {
		tx.db.removeTx(tx)
	}

	// Clear all references.
	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}

// Copy writes the entire database to a writer.
// This function exists for backwards compatibility.
//
// Deprecated; Use WriteTo() instead.
func (tx *Tx) Copy(w io.Writer) error {
	_, err := tx.WriteTo(w)
	return err
}

// WriteTo writes the entire database to a writer.
// If err == nil then exactly tx.Size() bytes will be written into the writer.
func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	// Attempt to open reader with WriteFlag
	f, err := os.OpenFile(tx.db.path, os.O_RDONLY|tx.WriteFlag, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// Generate a meta page. We use the same page data for both meta pages.
	buf := make([]byte, tx.db.pageSize)
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = metaPageFlag
	*page.meta() = *tx.meta

	// Write meta 0.
	page.id = 0
	page.meta().checksum = page.meta().sum64()
	nn, err := w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 0 copy: %s", err)
	}

	// Write meta 1 with a lower transaction id.
	page.id = 1
	page.meta().txid -= 1
	page.meta().checksum = page.meta().sum64()
	nn, err = w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 1 copy: %s", err)
	}

	// Move past the meta pages in the file.
	if _, err := f.Seek(int64(tx.db.pageSize*2), os.SEEK_SET); err != nil {
		return n, fmt.Errorf("seek: %s", err)
	}

	// Copy data pages.
	wn, err := io.CopyN(w, f, tx.Size()-int64(tx.db.pageSize*2))
	n += wn
	if err != nil {
		return n, err
	}

	return n, f.Close()
}

// CopyFile copies the entire database to file at the given path.
// A reader transaction is maintained during the copy so it is safe to continue
// using the database while a copy is in progress.
func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	err = tx.Copy(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Check performs several consistency checks on the database for this transaction.
// An error is returned if any inconsistency is found.
//
// It can be safely run concurrently on a writable transaction. However, this
// incurs a high cost for large databases and databases with a lot of subbuckets
// because of caching. This overhead can be removed if running on a read-only
// transaction, however, it is not safe to execute other writer transactions at
// the same time.
func (tx *Tx) Check() <-chan error {
	ch := make(chan error)
	go tx.check(ch)
	return ch
}

func (tx *Tx) check(ch chan error) {
	// Check if any pages are double freed.
	freed := make(map[pgid]bool)
	all := make([]pgid, tx.db.freelist.count())
	tx.db.freelist.copyall(all)
	for _, id := range all {
		if freed[id] {
			ch <- fmt.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	// Track every reachable page.
	reachable := make(map[pgid]*page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
		reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
	}

	// Recursively check buckets.
	tx.checkBucket(&tx.root, reachable, freed, ch)

	// Ensure all pages below high water mark are either reachable or freed.
	for i := pgid(0); i < tx.meta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {
			ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	// Close the channel to signal completion.
	close(ch)
}

func (tx *Tx) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	// Ignore inline buckets.
	if b.root == 0 {
		return
	}

	// Check every page used by this bucket.
	b.tx.forEachPage(b.root, 0, func(p *page, _ int) {
		if p.id > tx.meta.pgid {
			ch <- fmt.Errorf("page %d: out of bounds: %d", int(p.id), int(b.tx.meta.pgid))
		}

		// Ensure each page is only referenced once.
		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- fmt.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		// We should only encounter un-freed leaf and branch pages.
		if freed[p.id] {
			ch <- fmt.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- fmt.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	// Check each bucket within this bucket.
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

// allocate returns a contiguous block of memory starting at a given page.
func (tx *Tx) allocate(count int) (*page, error) {

	// 分配连续的 count 个内存页，返回首页指针 p
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// Save to our page cache.
	// 保存到内存缓冲中
	tx.pages[p.id] = p

	// Update statistics.
	// 更新统计信息
	tx.stats.PageCount++
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

// write writes any dirty pages to disk.
func (tx *Tx) write() error {

	// 1. 拷贝脏页字典 tx.pages 到 pages 中，以便进行排序
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}

	// 2. 清空脏页列表
	tx.pages = make(map[pgid]*page)

	// 3. 对 pages 按 id 从小到大排序
	sort.Sort(pages)

	// 4. 按照 id 从小到大的顺序写入到文件中
	for _, p := range pages {

		// 当前页面的大小（每个页面大小是确定的，这里直接乘就可以）
		size := (int(p.overflow) + 1) * tx.db.pageSize // p.overflow 表示当前 Page 是否有后续 Page，如果有，其表示后续页的数量，如果没有，则为0。

		// 当前页面位于磁盘文件中的偏移量
		offset := int64(p.id) * int64(tx.db.pageSize)

		// 将 p 转成字节数组，因为每次最多写入 maxAllocSize 字节，所以需要分次写入
		ptr := (*[maxAllocSize]byte)(unsafe.Pointer(p)) 	// Write out page in "max allocation" sized chunks.

		// 分次的将 p 写入到文件的 offset 偏移处，每次最多写入 maxAllocSize 字节
		for {

			// Limit our write to our max allocation size.
			sz := size
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}

			// Write chunk to disk.
			buf := ptr[:sz]
			if _, err := tx.db.ops.writeAt(buf, offset); err != nil {
				return err
			}

			// Update statistics.
			tx.stats.Write++

			// Exit inner for loop if we've written all the chunks.
			size -= sz
			if size == 0 {
				break
			}

			// Otherwise move offset forward and move pointer to next chunk.
			offset += int64(sz)
			ptr = (*[maxAllocSize]byte)(unsafe.Pointer(&ptr[sz]))
		}

	}

	// 5. 检查是否需要落盘，如果需要就 fsync
	if !tx.db.NoSync || IgnoreNoSync {
		// 默认每次写都调用 sync 落盘
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// 6. 把已写回的、独立的 page 放回到 pagePool 中
	for _, p := range pages {

		// 6.1 如果 p 是连续页面，则不予处理

		// Ignore page sizes over 1 page.
		// These are allocated using make() instead of the page pool.
		if int(p.overflow) != 0 {
			continue
		}

		// 6.2 如果 p 是单独页面，则清空页面（置0）
		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:tx.db.pageSize]
		// See https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
		for i := range buf {
			buf[i] = 0
		}

		// 6.3 将清空后的页面放回到 pagePool 中，留待重用
		tx.db.pagePool.Put(buf)

	}

	return nil
}

// writeMeta writes the meta to the disk.
//
// 步骤:
//  1. 转存 tx.meta 到 p 中
//  2. 把 p 写到磁盘中
//  3. fsync
//  4. 更新统计数据
func (tx *Tx) writeMeta() error {

	// 1. Create a temporary buffer for the meta page.
	buf := make([]byte, tx.db.pageSize)		// 创建 sizeOf(Page) 大小的 buf 用来存储 tx.meta
	p := tx.db.pageInBuffer(buf, 0)		// 将 buf 转换为 Page 结构体指针 p
	tx.meta.write(p) 						// 将 tx.meta 转存到 p 中

	// 2. Write the meta page to file.
	if _, err := tx.db.ops.writeAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {
		return err
	}

	// 3. Fsync
	if !tx.db.NoSync || IgnoreNoSync {
		// 默认每次写都调用 sync 落盘
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// 4. Update statistics.
	tx.stats.Write++
	return nil
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary buffered page is returned.
//
// 步骤:
//  1. 检查 id 页是否位于脏页 tx.pages[] 列表中，如果是，则直接返回；
//  2. 如果否，则直接从 mmap 中获取并返回。
func (tx *Tx) page(id pgid) *page {
	// 1. Check the dirty pages first.
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}
	// 2. Otherwise return directly from the mmap.
	return tx.db.page(id)
}

// forEachPage iterates over every page within a given page and executes a function.
//
//
//
func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {

	// 获取 id 页
	p := tx.page(pgid)

	// 执行 fn
	fn(p, depth)

	// 如果是当前页面是 '分支节点' 则需要递归处理子页面
	if (p.flags & branchPageFlag) != 0 {

		// 如果当前页面有连续页面，则每个页面都要递归处理
		for i := 0; i < int(p.count); i++ {

			//
			elem := p.branchPageElement(uint16(i))

			// 递归处理
			tx.forEachPage(elem.pgid, depth+1, fn)
		}

	}
}

// Page returns page information for a given page number.
// This is only safe for concurrent use when used by a writable transaction.
func (tx *Tx) Page(id int) (*PageInfo, error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	} else if pgid(id) >= tx.meta.pgid {
		return nil, nil
	}

	// 1. 从 db 中取 id 页
	p := tx.db.page(pgid(id))

	// 2. 构造 PageInfo 结构体，填充字段
	info := &PageInfo{
		ID:            id,
		Count:         int(p.count),
		OverflowCount: int(p.overflow),
	}

	// 3. 确定页面类型，检查该页面是否已经被 free ，然后填充 info.Type 字段 		// Determine the type (or if it's free).
	if tx.db.freelist.freed(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

// TxStats represents statistics about the actions performed by the transaction.
type TxStats struct {
	// Page statistics.
	PageCount int // number of page allocations
	PageAlloc int // total bytes allocated

	// Cursor statistics.
	CursorCount int // number of cursors created

	// Node statistics
	NodeCount int // number of node allocations
	NodeDeref int // number of node dereferences

	// Rebalance statistics.
	Rebalance     int           // number of node rebalances
	RebalanceTime time.Duration // total time spent rebalancing

	// Split/Spill statistics.
	Split     int           // number of nodes split
	Spill     int           // number of nodes spilled
	SpillTime time.Duration // total time spent spilling

	// Write statistics.
	Write     int           // number of writes performed
	WriteTime time.Duration // total time spent writing to disk
}

func (s *TxStats) add(other *TxStats) {
	s.PageCount += other.PageCount
	s.PageAlloc += other.PageAlloc
	s.CursorCount += other.CursorCount
	s.NodeCount += other.NodeCount
	s.NodeDeref += other.NodeDeref
	s.Rebalance += other.Rebalance
	s.RebalanceTime += other.RebalanceTime
	s.Split += other.Split
	s.Spill += other.Spill
	s.SpillTime += other.SpillTime
	s.Write += other.Write
	s.WriteTime += other.WriteTime
}

// Sub calculates and returns the difference between two sets of transaction stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.PageCount - other.PageCount
	diff.PageAlloc = s.PageAlloc - other.PageAlloc
	diff.CursorCount = s.CursorCount - other.CursorCount
	diff.NodeCount = s.NodeCount - other.NodeCount
	diff.NodeDeref = s.NodeDeref - other.NodeDeref
	diff.Rebalance = s.Rebalance - other.Rebalance
	diff.RebalanceTime = s.RebalanceTime - other.RebalanceTime
	diff.Split = s.Split - other.Split
	diff.Spill = s.Spill - other.Spill
	diff.SpillTime = s.SpillTime - other.SpillTime
	diff.Write = s.Write - other.Write
	diff.WriteTime = s.WriteTime - other.WriteTime
	return diff
}
