package bolt

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// The largest step that can be taken when remapping the mmap.
const maxMmapStep = 1 << 30 // 1GB

// The data file format version.
const version = 2

// Represents a marker value to indicate that a file is a Bolt DB.
const magic uint32 = 0xED0CDAED

// IgnoreNoSync specifies whether the NoSync field of a DB is ignored when
// syncing changes to a file.  This is required as some operating systems,
// such as OpenBSD, do not have a unified buffer cache (UBC) and writes
// must be synchronized using the msync(2) syscall.

const IgnoreNoSync = runtime.GOOS == "openbsd"

// Default values if not set in a DB instance.
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	DefaultAllocSize         = 16 * 1024 * 1024
)

// default page size for db is set to the OS page size.
var defaultPageSize = os.Getpagesize()

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {

	// 启用后，数据库每次提交都会进行一次 Check()，当数据库状态不一致时，就会 panic 。
	// StrictMode 对性能影响很大，建议在调试模式下才使用。
	//
	// When enabled, the database will perform a Check() after every commit.
	// A panic is issued if the database is in an inconsistent state. This
	// flag has a large performance impact so it should only be used for
	// debugging purposes.
	StrictMode bool


	// Setting the NoSync flag will cause the database to skip fsync() calls after each commit.
	// This can be useful when bulk loading data into a database and you can restart the bulk load
	// in the event of a system failure or database corruption. Do not set this flag for normal use.
	//
	// If the package global IgnoreNoSync constant is true, this value is ignored.
	// See the comment on that constant for more details.
	//
	// THIS IS UNSAFE. PLEASE USE WITH CAUTION.
	NoSync bool


	// When true, skips the truncate call when growing the database.
	// Setting this to true is only safe on non-ext3/ext4 systems.
	// Skipping truncation avoids preallocation of hard drive space and
	// bypasses a truncate() and fsync() syscall on remapping.
	//
	// https://github.com/boltdb/bolt/issues/284
	NoGrowSync bool



	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	MmapFlags int




	// MaxBatchSize is the maximum size of a batch. Default value is copied from DefaultMaxBatchSize in Open.
	//
	// If <=0, disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchSize int




	// MaxBatchDelay is the maximum delay before a batch starts.
	// Default value is copied from DefaultMaxBatchDelay in Open.
	//
	// If <=0, effectively disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchDelay time.Duration




	// AllocSize is the amount of space allocated when the database
	// needs to create new pages. This is done to amortize the cost
	// of truncate() and fsync() when growing the data file.
	AllocSize int





	path     string
	file     *os.File
	lockfile *os.File // windows only





	// 这三个带 data 的字段应该和内存映射有关系
	dataref  []byte  			 // mmap'ed readonly, write throws SEGV
	data     *[maxMapSize]byte
	datasz   int





	filesz   int // current on disk file size
	meta0    *meta
	meta1    *meta
	pageSize int
	opened   bool

	rwtx     *Tx    //读写事务，全局一个
	txs      []*Tx  //只读事务，全局多个




	freelist *freelist
	stats    Stats




	pagePool sync.Pool




	batchMu sync.Mutex
	batch   *batch





	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.





	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	readOnly bool
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open creates and opens a database at the given path.
// If the file does not exist then it will be created automatically.
// Passing in nil options will cause Bolt to open the database with the default options.
//
// Open() 执行的主要步骤为:
//	1. 创建 DB 对象，并将其状态设为 opened ；
//	2. 打开或创建文件对象
//	3. 根据 Open 参数 ReadOnly 决定是否以进程独占的方式打开文件:
//		如果以只读方式访问数据库文件，则不同进程可以共享读该文件；
//		如果以读写方式访问数据库文件，则文件锁将被独占，其他进程无法同时以读写方式访问该数据库文件，这是为了防止多个进程同时修改文件；
//	4. 初始化写文件函数；
//	5. 读数据库文件：
//		如果文件大小为零，则对db进行初始化；
//		如果大小不为零，则试图读取前 4K 个字节来确定当前数据库的 pageSize ，若失败则使用系统的pagesize作为db.pageSize。
//	6. 通过 mmap 对打开的数据库文件进行内存映射，并初始化 db 对象中的 meta 指针；
//	7. 读数据库文件中的 freelist 页，并初始化 db 对象中的 freelis t列表。freelist 列表中记录着数据库文件中的空闲页。

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {

	// 创建数据库对象，并将状态设置为 opened
	var db = &DB{opened: true}

	// 设置缺省配置项
	if options == nil {
		options = DefaultOptions
	}

	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	// Set default values for later DB operations.
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// Open data file and separate sync handler for metadata writes.
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// Lock file so that other processes using Bolt in read-write mode cannot
	// use the database  at the same time. This would cause corruption since
	// the two processes would write meta pages and free pages separately.
	// The database file is locked exclusively (only one process can grab the lock)
	// if !options.ReadOnly.
	// The database file is locked using the shared lock (more than one process may
	// hold a lock at the same time) otherwise (options.ReadOnly is set).
	// 数据库有读写操作，可能有多个进程同时写一个文件，bolt 使用文件锁来防止多个进程同时对 db.file 文件进行并发读写。
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// Default values for test hooks
	// 默认的写入方式为文件写
	db.ops.writeAt = db.file.WriteAt

	// Initialize the database if it doesn't exist.
	// 1. 若数据文件 stat 信息无法获取，则报错
	if info, err := db.file.Stat(); err != nil {
		return nil, err

	// 2. 文件大小为 0 ，代表是新文件，需要初始化
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return nil, err
		}

	// 3. 文件状态正常，读取 meta 页来确定 PageSize 大小
	} else {

		// Read the first meta page to determine the page size.
		// 十六进制 0x1000 等于十进制的 4096，也即读取文件的开头 4096 字节
		var buf [0x1000]byte

		// 读取文件 db.file 开头的第一个 Page 的前 4KB ，当作 Meta 页来解析其 data 部分，来获取 PageSize 。
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				// If we can't read the page size, we can assume it's the same
				// as the OS -- since that's how the page size was chosen in the
				// first place.
				//
				// If the first page is invalid and this OS uses a different
				// page size than what the database was created with then we
				// are out of luck and cannot access the database.
				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}


	// Initialize page pool.
	// 初始化一个对象资源池，以减少GC提升性能。
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// Memory map the data file.
	// mmap 内存映射
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// Read in the freelist.
	db.freelist = newFreelist()
	db.freelist.read(db.page(db.meta().freelist))

	// Mark the database as opened and return.
	return db, nil
}


// 从 Init() 函数中可知，创建文件的时候至少创建了 4 个 pageSize ，
// 而且 options 的默认 InitialMmapSize 没有指定，因此判断一次映射多大的时候就是用文件大小进行判断。
//
// 文件映射的大小从 32K 逐次翻倍直到 1G ，然后如果文件大小大于 1G ，就直接每次增加 1G 。
// 在对文件进行映射前，先解除引用，然后再进行映射。
// 我们之前知道，文件的前两页是 metaPage ，因此读取出来即可。




// db.mmap() 在两种情况下会被调用:
// (1) 数据文件创建或打开后进行第一次内存映射时；
// (2) 在写入数据库后且数据库文件要增大时，分配新的页后，需要重新进行 mmap 系统调用将新的文件范围映射入进程地址空间。



// 在情况 1 中，还没有开始 db.beginTx() 的调用，故不存在 db.mmaplock 锁争用问题；
// 在情况 2 中，数据库可能被不同的线程读写，可能存在某线程中的读写 transaction 写入了大量数据，在 Commit 时，
// 由于当前已映射区的空闲页不够，会调用 db.mmap() 重新进行内存映射，此时若有未关闭的只读 transaction，由于它占用着在 db.mmaplock 的读锁，
// db.mmap()会阻塞在争用db.mmaplock写锁的地方。也就是说，如果存在着耗时的只读transaction， 同时写transaction需要remmap时，写操作会被读操作阻塞。
// 由此可以看出，使用BoltDB时，应尽量避免耗时的读操作， 同时在写操作时应避免频繁地remmap，我们将在介绍BoltDB的MVCC机制时再讨论这个问题 。


//在 db.mmap() 中:
//
// 1. 获取 db 对象的 mmaplock ，意思是从文件往内存映射的时候，一次只允许映射一次。
// 2. 通过 db.mmapSize() 确定 mmap 映射文件的长度，因为 mmap 系统调用时要指定映射文件的起始偏移和长度，即确定映射文件的范围；
// 3. 通过 munmap() 将老的内存映射 unmap ;
// 4. 通过 mmap() 将文件映射到内存，完成后可以通过 db.data 来读文件内容了;
// 5. 读数据库文件的第 0 页和第 1 页来初始化 db.meta0 和 db.meta1 ，前面 init() 方法中我们了解到 db 的第 0 面和第 1 页写入的是 meta ；
// 6. 对 meta 数据进行校验。
//

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {

	// 1. 加锁
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	// 2. 文件的长度
	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}

	// 根据 size 确定合适的 mmap 大小
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// Dereference all mmap references before unmapping.
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// Unmap existing data before continuing.
	// 如果内存中已经映射了，清除映射
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	// 真正的开始映射
	if err := mmap(db, size); err != nil {
		return err
	}

	// Save references to the meta pages.
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.


	// 验证 meta page 的有效性。
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()

	// 在两者都失效的情况下，才报错；一个失效的话，可以从另一个进行恢复。
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// munmap unmaps the data file from memory.
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// db.mmapSize() 的实现比较简单，它的思想是：
// 映射文件的最小 size 为 32KB ，当文件小于 1G 时，它的大小以加倍的方式增长，
// 当文件大于 1G 时，每次 remmap 增加大小时，是以 1G 为单位增长的。
//
// 前述init()调用完毕后，文件大小是 16KB ，即 db.mmapSize 的传入参数是 16384 ，由于 mmapSize() 限制最小映射文件大小是 32768，
// 故它返回的 size 值为 32768 ，在随后的 mmap() 调用中第二个传入参数便是 32768 ，即 32K 。
//
// 但此时文件大小才16KB，这个时候映射 32KB 的文件会不会有问题？window平台和linux平台对此有不同的处理:
// 在针对 windows 平台的实现中，在进行 mmap 映射之前都会通过 ftruncate 系统调用将文件大小调整为待映射的大小，
// 而在 linux/unix 平台的实现中是直接进行mmap调用的：



// 我们知道，mmap 也是以页为单位进行映射的，
// (1) 如果文件大小不是页大小的整数倍，映射的最后一页肯定超过了文件结尾处，这个时候超过部分的内存会初始化为 0 ，对其的写操作不会写入文件。
// (2) 但如果映射的内存范围超过了文件大小，且超出范围大于4k，那对于超过文件所在最后一页地址空间的访问将引发异常。
//
// 比如我们这里文件实际大小是16K，但我们要映射 32K 到进程地址空间中，那对超过 16K 部分的内存访问将会引发异常。
//
// 实际上，我们前面分析过，Boltdb 通过 mmap 进行了只读映射，故不会存在通过内存映射写文件的问题，
// 同时，对 db.data (即映射的内存区域)的访问是通过 pgid 来访问的，当前 database 文件里实际包含多少个 page 是记录在 meta 中的，
// 每次通过 db.data 来读取一页时，boltdb 均会作超限判断的，所以不会存在对超过当前文件实际页数以外的区域访问的情况。
// 正如我们在 db.init() 中看到的，此时 meta 中记录的 pgid 为 4 ，即当前数据库文件总的 page 数为 4 ，故即使 mmap 映射长度为 32KB，
// 通过 pgid 索引也不会访问到 16KB 以外的地址空间。
//
// 需要说明的是，当对数据库进行写操作时，如果要增加文件大小，针对linux/unix系统，boltdb 也会通过 ftruncate 系统调用增加文件大小，
// 但是它并不是为了避免访问映射区域发生异常的问题，因为 boltdb 写文件不是通过 mmap ，而是直接通过 fwrite 写文件。
// 强调一下，boltdb 对数据库的读操作是通过读 mmap 内存映射区完成的；而写操作是通过文件 fseek 及 fwrite 系统调用完成的。



// 1 Kb = 1024 b 	 = 2^10 bytes
// 1 Mb = 1024 Kb    = 2^20 bytes
// 1 Gb = 1024 Mb    = 2^30 bytes
//
// 1<<3 ，相当于 1 * 2^3
//
//

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
func (db *DB) mmapSize(size int) (int, error) {

	// Double the size from 32KB until 1GB.
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	sz := int64(size)

	// If larger than 1GB then grow by 1GB at a time.
	// 如果大于 1GB 则每次增长 1GB。
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	// 确保 mmap 的大小是 pageSize 的整数倍
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// init creates a new database file and initializes its meta pages.


// init() 方法创建了一个空的数据库，通过它，我们可以了解 boltdb 数据库文件的基本格式：
//
// 1. 数据库文件以页为基本单位，一个数据库文件由若干页组成。
// 2. 一个页的大小是由当前 OS 决定的，是确定的，通过 os.GetpageSize() 来获取。对于32位系统，值一般为4KB。
// 3. 数据库文件的前两页是 meta 页，第三页是记录 freelist 的页面，第 4 页及后续各页则是用于存储 K/V。
// 	  事实上，经过若干次读写后，freelist 页并不一定会存在第三页，也可能不止一页。


// 具体来说，存储格式大概是：disk: [P1|P2|P3|P4|...|Pn]

// 1. P1 和 P2 都是META_PAGE，且相互独立，设置两个META_PAGE的原因是用于备份。
//  下面介绍 MEGA_PAGE 中一些关键字段, 并可以从中看出 BoltDB 的一些机制;
//	root_pgid: BoltDB 利用分页, 在磁盘维护一个 B+ 树, 该字段表示 B+ 树根节点所在的页号;
//	freelist_pgid: FREELIST_PAGE 所在的页号;
//	checksum: 校验码;
//
// 2. FREELIST_PAGE 就是一个数组, 记录所有了 free page 的页号; FREELIST_PAGE 用来持久化这个数据;
//
// 3. DATA_PAGE 持久化 B+ 树的节点, 用来维护索引和数据;


// 分别用MP, FP, DP来区分表示这些页, 则磁盘的格式大概是：disk: [MP1|MP2|DP1|DP2|...|DPi-1|FP|DPi|DPi+1|...]
// 任何页都有可能作为 FREELIST PAGE 使用, 在 META PAGE 中的 freelist_pgid 记录了它当前的位置;


// 由于内存有限, 一般分页后, 都需要一个 PageCache 之类的组件, 来进行分页的置换; BoltDB 直接使用 mmap , 直接将所有的页,
// 也就是整个数据大文件, 全部映射到内存内，格式大概是：
//
//  mem: [MP1|MP2|DP1|DP2|...|DPi-1|FP|DPi|DPi+1|...]
//						   ^
//						   |(mmap)
//						   |
// disk: [MP1|MP2|DP1|DP2|...|DPi-1|FP|DPi|DPi+1|...]
//
// mmap 把分页的管理交给了操作系统，从而省略了自己实现 PageCache 的麻烦;








// 在init()中:
//
//	1. 先分配了4个 page 大小的 buffer ；
//	2. 将第 0 页和第 1 页初始化为 meta 页，并指定 root bucket 的 page id 为 3 ，存 freelist 记录的 page id 为 2 ，
//		当前数据库总页数为 4 ，同时 txid 分别为 0 和 1 。
//	3. 将第 2 页初始化为 freelist 页，即 freelist 的记录将会存在第 2 页；
//	4. 将第 3 页初始化为一个空页，它可以用来写入 K/V 记录，请注意它必须是 B+ Tree 中的叶子节点；
//	5. 最后，调用写文件函数将 buffer 中的数据写入文件，同时通过 fdatasync() 调用将内核中磁盘页缓冲立即写入磁盘。


// init() 执行完毕后，新创建的数据库文件大小将是 16K 字节，随后 Open() 方法便调用 db.mmap() 方法对该文件进行映射。

func (db *DB) init() error {


	// 1. 先分配了 4 个 page 大小的 buffer

	// Set the page size to the OS page size.
	db.pageSize = os.Getpagesize()

	// Create two meta pages on a buffer.
	buf := make([]byte, db.pageSize*4)


	// 2. 将第 0 页和第 1 页初始化为 meta 页
	for i := 0; i < 2; i++ {

		p := db.pageInBuffer(buf[:], pgid(i))	// 从 buf 中取出第 i 页对应的 Page 结构体
		p.id = pgid(i)							// 设置 PageID
		p.flags = metaPageFlag 					// meta 页

		// Initialize the meta page.
		m := p.meta()
		m.magic = magic							// 幻数
		m.version = version						// 版本号
		m.pageSize = uint32(db.pageSize)		// 页大小
		m.freelist = 2							// 指定存储 freelist 记录的 page id 为 2
		m.root = bucket{root: 3} 				// 指定 root bucket 的 page id 为 3
		m.pgid = 4 								// 指定 Boltdb 文件总页数为 4
		m.txid = txid(i)						//
		m.checksum = m.sum64()              	// 页校验和
	}







	// 3. 将第 2 页初始化为 freelist 页，即 freelist 的记录将会存储在第 2 页
	// Write an empty freelist at page 3.
	p := db.pageInBuffer(buf[:], pgid(2)) 	// 从 buf 中取出第 2 页对应的 Page 结构体
	p.id = pgid(2) 							// PageID
	p.flags = freelistPageFlag 				// freelist 页
	p.count = 0 							// 记录数为 0

	// 4. 将第 3 页初始化为一个空页，它可以用来写入 K/V 记录，请注意它必须是 B+ Tree 中的叶子节点
	// Write an empty leaf page at page 4.
	p = db.pageInBuffer(buf[:], pgid(3)) 	// 从 buf 中取出第 3 页对应的 Page 结构体
	p.id = pgid(3)							// PageID
	p.flags = leafPageFlag 					// 叶子节点
	p.count = 0 							// 记录数为 0


	// 5. 调用写文件函数将 buf 中的数据写入到文件 db.file 的 offset 位置。
	// Write the buffer to our data file.
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}

	// 6. 通过 fdatasync() 调用将内核中磁盘页缓冲立即写入磁盘
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {

	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {

	// 已关闭则退出
	if !db.opened {
		return nil
	}

	// 设置关闭标识
	db.opened = false

	// 重置空闲列表
	db.freelist = nil

	// Clear ops.
	db.ops.writeAt = nil

	// Close the mmap.
	if err := db.munmap(); err != nil {
		return err
	}

	// Close file handles.
	if db.file != nil {

		// No need to unlock read-only file.
		// 解锁文件
		if !db.readOnly {
			// Unlock the file.
			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
			}
		}

		// Close the file descriptor.
		// 关闭文件描述符，调用 golang 的文件关闭函数 file.Close()
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

// Begin starts a new transaction.
// Multiple read-only transactions can be used concurrently but only one
// write transaction can be used at a time. Starting multiple write transactions
// will cause the calls to block and be serialized until the current write
// transaction finishes.
//
// Transactions should not be dependent on one another. Opening a read
// transaction and a write transaction in the same goroutine can cause the
// writer to deadlock because the database periodically needs to re-mmap itself
// as it grows and it cannot do that while a read transaction is open.
//
// If a long running read transaction (for example, a snapshot transaction) is
// needed, you might want to set DB.InitialMmapSize to a large enough value
// to avoid potential blocking of write transaction.
//
// IMPORTANT: You must close read-only transactions after you are finished or
// else the database will not reclaim old pages.




// 如果传入的 writeable 为 true， 则调用 db.beginRWTx() 创建一个可写的 transaction ；
// 如果传入的 writeable 为 false，则调用 db.beginTx()   创建一个只读的 transaction 。

func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}




// 在 bolt 中，创建一个只读事务，其实并不会增加其 txn id ，所以，一个只读事务相当于是指向了当前最近一次完成的读写事务的状态，
// 或者说，指向了 db 在这个只读事务打开的那个时间点的一个 snapshot 。





func (db *DB) beginTx() (*Tx, error) {

	// 1. 获取 db.metalock，因为后面要对 db 对象进行读写;

	// Lock the meta pages while we initialize the transaction.
	// We obtain the meta lock before the mmap lock because that's the order
	// that the write transaction will obtain them.
	db.metalock.Lock()

	// 2. 获取 db.mmaplock 读锁

	// Obtain a read-only lock on the mmap.
	// When the mmap is remapped it will obtain a write lock so all transactions must finish before it can be remapped.
	db.mmaplock.RLock()


	// Exit if the database is not open yet.
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	t := &Tx{}
	t.init(db)

	// Keep track of transaction until it closes.
	// 创建只读事务时，会将事务 t 追加到 db.txs 中。
	db.txs = append(db.txs, t)


	n := len(db.txs)

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Update the transaction stats.
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}







// 在db.beginRWTx()中:
//
// 1. 获取读写锁，db.rwlock 只有在 transaction 被 Commit 或者 Rollback 的时候释放，
// 	  也即它将锁定读写 transaction 的整个生命周期，实现了一个进程内同时只有一个读写 transaction 。
// 	  请注意，虽然它的名字叫 rwlock ，但它并不是读写锁，而是一个互斥锁(sync.Mutex)。
//
// 	调用 bolt.Open() 方法打开数据库文件时，如果以读写的方式打开，文件锁将会被独占，防止同时有多个进程写文件。
//  结合文件锁与 db.rwlock ，BoltDB 可以保证同一时段只有一个进程的一个线程可以对数据库修改。
//  如果在Go中调用，可以认为只有一个 goroutine 会修改数据库，尽管一个 goroutine 可能会被调度到不同的内核线程上。
//  这里大家可能会对它的 MVCC 支持有疑问，这里先不讨论，待介绍完它的工作机制后我们再讨论。

// 2. 获取 db.metalock ，需要注意的是，metalock 实际上是对 db 对象的访问保护，特别是对 db.txs 的读写保护，而不是如名字或者注释中说的专门对 meta page 的读写保护。

// 3. 新建一个 Tx 对象，并通过 t 来引用，随后调用 t.init() 方法来对刚刚创建的读写 Tx 进行初始化;
// 4. 将 db.rwtx 设为刚刚创建并初始化的事务 t ，写事务全局只有一个。
// 5. db.txs 字段用来记录所有的已打开的只读 transaction ，它是一个map，从这里也可以看出，BoltDB 同时只能有一个可读写 transaction，但可以有多个只读transactions;


func (db *DB) beginRWTx() (*Tx, error) {

	// If the database was opened with Options.ReadOnly, return an error.
	// 在只读模式下，不允许开启事务。
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	// Obtain writer lock. This is released by the transaction when it closes.
	// This enforces only one writer transaction at a time.
	db.rwlock.Lock()

	// Once we have the writer lock then we can lock the meta pages so that we can set up the transaction.
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	t := &Tx{
		writable: true,
	}
	t.init(db)
	db.rwtx = t

	// Free any pages associated with closed read-only transactions.
	var minid txid = 0xFFFFFFFFFFFFFFFF

	// 遍历 db.txs 中记录的所有只读事务，取出其中最小的事务 id 即 minid，所有小于 minid 的缓存都已经失效，可以清理。
	for _, t := range db.txs {
		if t.meta.txid < minid {
			minid = t.meta.txid
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}


// removeTx removes a transaction from the database.
func (db *DB) removeTx(tx *Tx) {
	// Release the read lock on the mmap.
	db.mmaplock.RUnlock()

	// Use the meta lock to restrict access to the DB object.
	db.metalock.Lock()

	// Remove the transaction.

	for i, t := range db.txs {
		if t == tx {
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Merge statistics.
	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()
}

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.


// db.Update()方法主要执行:
// 1. 通过 db.Begin(writeable bool) 创建一个读写的事务对象，返回 t * Tx 指针;
// 2. 调用传入的函数 fn(t)，该函数中依赖 t 指向的 Transaction 对象进行数据库的创建、查找、删除及遍历操作。
// 3. 调用 t.Commit() 将对 BoltDB 的修改提交并写入磁盘;
// 4. 如果 fn(t) 返回 error，或者在 t.Commit的时候发生异常或错误，执行回滚 t.Rollback() ;

func (db *DB) Update(fn func(*Tx) error) error {

	// 1. 创建一个可读写的 Transaction 事务对象
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()


	// Mark as a managed tx so that the inner function cannot manually commit.
	t.managed = true

	// If an error is returned from the function then rollback and return error.
	err = fn(t)

	t.managed = false

	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually rollback.
	t.managed = true

	// If an error is returned from the function then pass it through.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	if err := t.Rollback(); err != nil {
		return err
	}

	return nil
}

// Batch calls fn as part of a batch. It behaves similar to Update,
// except:
//
// 1. concurrent Batch calls can be combined into a single Bolt
// transaction.
//
// 2. the function passed to Batch may be called multiple times,
// regardless of whether it returns error or not.
//
// This means that Batch function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
//
// The maximum batch size and delay can be adjusted with DB.MaxBatchSize
// and DB.MaxBatchDelay, respectively.
//
// Batch is only useful when there are multiple goroutines calling it.
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// There is no existing batch, or the existing batch is full; start a new one.
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// trigger runs the batch if it hasn't already been run.
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// Make sure no new work is added to this batch, but don't break
	// other batches.
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// take the failing transaction out of the batch. it's
			// safe to shorten b.calls here because db.batch no longer
			// points to us, and we hold the mutex anyway.
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// tell the submitter re-run it solo, continue with the rest of the batch
			c.err <- trySolo
			continue retry
		}

		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}



// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by callers.
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}


func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Sync executes fdatasync() against the database file handle.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database file to sync against the disk.
func (db *DB) Sync() error {
	// 刷到 DB
	return fdatasync(db)
}


// Stats retrieves ongoing performance stats for the database.
// This is only updated when a transaction closes.
func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

// This is for internal access to the raw data bytes from the C cursor, use carefully, or not at all.
func (db *DB) Info() *Info {
	return &Info{
		uintptr(unsafe.Pointer(&db.data[0])),
		db.pageSize,
	}
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id pgid) *page {
	// buffer := db.data[]
	// offset := id*sizeof(PageSize)
	// addr   := &db.data[offset]
	// p      := (*page)addr
	//
	// return p
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	// offset := id*sizeof(PageSize)
	// size := PageSize
	// res := (*page)b[offset...offset+size]
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}


// db.meta() 返回的是两个 meta 中 txid 更大且通过校验的那个，
// 前面我们说 meta 中的 txid 可以看作是数据库的修改版本号，
// 所以 db.meta() 返回的 meta 对应的是数据库最新的状态。
//
// 注意，db.meta() 返回的是 *meta 指针，也即返回的 meta 信息可能会被更新。

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {

	// We have to return the meta with the highest txid which doesn't fail validation.
	// Otherwise, we can cause errors when in fact the database is in a consistent state.
	// metaA is the one with the higher txid.

	metaA := db.meta0
	metaB := db.meta1

	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	// Use higher meta page if valid. Otherwise fallback to previous, if valid.
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate returns a contiguous block of memory starting at a given page.
// 分配连续的 count 个内存页。
func (db *DB) allocate(count int) (*page, error) {

	// 1. 分配 count * sizeOfPage 的 buf
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte) // db.pagePool 是 sync.pool，缓存了大小为 page size 的 buffer
	} else {
		buf = make([]byte, count*db.pageSize)
	}

	// 2. 将 buf 转化成 Page 结构体
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1) // p.overflow 表示当前页面 p 是否有后续页面；如果有，表示后续页的数量，如果没有，则为0。

	// 3.
	// Use pages from the freelist if they are available.
	if p.id = db.freelist.allocate(count); p.id != 0 { // db.freelist.allocate() 查找合适的连续的 page，返回首 page id
		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// Move the page id high water mark.
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// grow grows the size of the database to the given sz.
func (db *DB) grow(sz int) error {
	// Ignore if the new size is less than available file size.
	if sz <= db.filesz {
		return nil
	}

	// If the data is smaller than the alloc size then only allocate what's needed.
	// Once it goes over the allocation size then allocate in chunks.
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

// Options represents the options that can be set when opening a database.
type Options struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	Timeout time.Duration

	// Sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync bool

	// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly bool

	// Sets the DB.MmapFlags flag before memory mapping the file.
	MmapFlags int

	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <=0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	InitialMmapSize int
}

// DefaultOptions represent the options used if nil options are passed into Open().
// No timeout is used which will cause Bolt to wait indefinitely for a lock.
var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}




// Stats represents statistics about the database.
type Stats struct {

	// Freelist stats
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist


	// Transaction stats
	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions


	TxStats TxStats // global, ongoing stats.
}

// Sub calculates and returns the difference between two sets of database stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

func (s *Stats) add(other *Stats) {
	s.TxStats.add(&other.TxStats)
}

type Info struct {
	Data     uintptr
	PageSize int
}

// meta page 的结构
type meta struct {
	magic uint32		// magic number，为0xED0CDAED
	version uint32		// 文件格式的版本号，为2
	pageSize uint32 	// 页的大小
	flags uint32 		// 保留字段，暂时没有用到
	root bucket			//
	freelist pgid 		// freelist 用来存空闲页面的页号
	pgid pgid 			// boltdb 文件中的总页数，即最大页号加1。
	txid txid			// 上一次写数据库的 transcation id，可以看作是当前 boltdb 的修改版本号，每次读写数据库时加1，只读时不改变；
	checksum uint64 	// 上面各字段的 64位 FNV-1 哈希校验
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
// 验证 mata page 的有效性
// 	1. 幻数
//	2. 版本号
//	3. 校验和
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// 1. 确定写入的 meta 页号，是 0 还是 1 。
// 指定写入的页号为 m.txid % 2，即第 0 或者第 1 页，这与我们之前看到的第 0 页或者第 1 页初始化为meta页是相符的。
// 更重要的是，写入第 0 页还是第 1 页是由当前 meta 中的 transction id 决定的，
// 若当前 meta 页的 transaction id 为偶数则写入第0页，
// 若当前 meta 页的 transaction id 为奇数则写入第1页。
//
// 前面介绍说 meta 中的 txid 实际上可以看作是数据库的修改版本号，每次写时会增加1，也就是说每次写数据库后会交替更新 meta 页。
// 如当前 txid 为 10 ，它对应的 meta 存在第 0 页，当对数据库进行一次读写时，txid 增加为 11 ，写完后需要更新 meta 页，
// 这时会将新的 meta 写入第 1 页，而不是覆盖原来的第 0 页，下次读写数据库时将会选择 txid 更大的 meta 页来提取 meta 信息。
//
// 我们后面介绍读写数据库时会进一步介绍，这里先提一个问题供大家思考:
// boltdb 为什么要维护两页 meta (让我们称之为双 meta )呢？
//
// 2. 将页面 flags 设定为 metaPageFlag ，指明为一个 meta 页面;
// 3. 将 meta 信息拷贝到页面 p 中缓存的相应位置，我们来看看这个位置是如何确定的:


// write writes the meta onto a page.
// 将 meta 页 m 转存为 p
func (m *meta) write(p *page) {

	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// page id 可以是 0 或者 1 ，我们可以通过 事务ID 是奇数还是偶数来确定。
	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.

	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	// Calculate the checksum.
	m.checksum = m.sum64()

	m.copy(p.meta())
}

// generates the checksum for the meta.
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}
