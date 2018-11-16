#include "lib.h"
#include <pthread.h>

#define   MAX_OBJS            1000005
#define   MAX_BLOCKS          8388608
#define   MAX_SIZE            4096 // max size of a file defined by the number of pages
#define   INODE_BITMAP_START  0
#define   INODE_BITMAP_END    30
#define   INODE_BITMAP_CNT    31
#define   BLOCK_BITMAP_START  31
#define   BLOCK_BITMAP_END    286
#define   BLOCK_BITMAP_CNT    256
#define   INODE_START         287
#define   INODE_END           20286
#define   INODE_CNT           20000
#define   NUM_BITS            32
#define   BLOCKS_IN_INDIRECT  1024
#define   NUM_DIRECT_BLOCKS   4
#define   NUM_INDIRECT_BLOCKS 4
#define   NUM_CACHE_BLOCKS    32704


/*  this represent the structure of an inode
	It contain the i-node id, size of this file etc
	file is matched using key
	4 direct block provide 16k memory, while 4 indirect point to 4 block storing integer offset
		of 4 * 1024 = 4096 entries = 16 MB space
	blocks to store this 16 MB, indirect pointers are created on demand, no memory is declared before hand
*/
struct object {
	int id;
	int size;
	int cache_index;
	int dirty;
	int direct[4];
	char key[33];
	int indirect[4];
};

/*
Cache implementation follows a simple rule
if we want to store block no x, then store it it cache + x%MAX_CACHE
if this place is already filled and existing block is dirty, then copy that data to disk
and introduce this new page.

write is done directly to cache and dirty bit is set to 1
*/
struct cache_struct {
	int dirty;
	int block_no;
	char data[BLOCK_SIZE];
};

struct object *curr;
struct object *objs; // this pointer stores the inodes in sequencial manner
int *BLOCK_BITMAP;
int *INODE_BITMAP;
int NUM_BLOCKS; // Global variable to store number of blocks
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void *malloc_mp(int sz) {
	void *tmp = mmap(NULL, (size_t)sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if(tmp == MAP_FAILED)
		return NULL;
	return tmp;
}

void free_mp(void *ptr, int sz) {
	munmap(ptr, (size_t)sz);
	return ;
}

/* sync the cache of this block with disk
   checks if the page is indeed present, and if it is dirty
*/
int obj_sync(struct objfs_state *objfs,int cache_no) {
	struct cache_struct *cache = ((struct cache_struct *)objfs -> cache) + cache_no;
	if(!cache->block_no)
		return 0;
	else if(!cache->dirty)
		return 0;
	void *ptr = malloc_mp(BLOCK_SIZE);
	if(!ptr)
		return -1;
	memcpy(ptr, cache->data, BLOCK_SIZE);
	if(write_block(objfs, cache->block_no, ptr) < 0)
		return -1;
	free_mp(ptr, BLOCK_SIZE);
	cache->dirty = 0;
	return 0;
}

int cache_init(struct objfs_state *objfs,int cache_no) {
	struct cache_struct *cache = ((struct cache_struct *)objfs -> cache) + cache_no;
	cache->block_no = 0;
	cache->dirty = 0;
	return 0;
}

#ifdef CACHE
int find_write(struct objfs_state *objfs, int block_id, const char *user_buf, int size) {
	struct cache_struct *cache = ((struct cache_struct *)objfs -> cache) + (block_id % NUM_CACHE_BLOCKS);
	if(cache->block_no != block_id) { // some other block or empty
		if(cache->dirty)
			obj_sync(objfs, block_id%NUM_CACHE_BLOCKS);
	}
	cache->block_no = block_id;
	cache->dirty = 1;
	memcpy(cache->data, user_buf, size);
	return 0;
}

int find_read(struct objfs_state *objfs, int block_id, char *user_buf, int size) {
	struct cache_struct *cache = ((struct cache_struct *)objfs -> cache) + (block_id % NUM_CACHE_BLOCKS);
	if(cache->block_no == block_id) { // cache hit
		memcpy(user_buf, cache->data, size);
		return 0;
	}
	else {
		// load this into cache
		if(cache->block_no != 0 && cache->dirty) {
			obj_sync(objfs, block_id%NUM_CACHE_BLOCKS);
		}
		cache->block_no = block_id;
		void *ptr = malloc_mp(BLOCK_SIZE);
		if(!ptr)
			return -1;
		if(read_block(objfs, block_id, ptr) < 0)
			return -1;
		memcpy(user_buf, ptr, size);
		memcpy(cache->data, ptr, BLOCK_SIZE);
		free_mp(ptr, BLOCK_SIZE);
	}
	return 0;
}
#else
int find_write(struct objfs_state *objfs, int block_id, const char *user_buf, int size) {
	void *ptr = malloc_mp(BLOCK_SIZE);
	if(!ptr)
				return -1;
	memcpy(ptr, user_buf, size);
	if(write_block(objfs, block_id, ptr) < 0)
			return -1;
	free_mp(ptr, BLOCK_SIZE);
	return 0;
}

int find_read(struct objfs_state *objfs, int block_id, char *user_buf, int size) {
	void *ptr = malloc_mp(BLOCK_SIZE);
	if(!ptr)
		return -1;
	if(read_block(objfs, block_id, ptr) < 0)
		return -1;
	memcpy(user_buf, ptr, size);
	free_mp(ptr, BLOCK_SIZE);
	return 0;
}
#endif

/*
Returns the object ID.  -1 (invalid), 0, 1 - reserved
*/
long find_object_id(const char *key, struct objfs_state *objfs) {
	struct object *obj = objs;
	int bitmap_sz = (INODE_BITMAP_CNT * BLOCK_SIZE) / 4;
	for(int i = 0; i < bitmap_sz; i++) {
		int mask = 1;
		for(int j = 0; j < NUM_BITS; j++) {
			if((INODE_BITMAP[i] & mask) && !strcmp(obj->key, key)) {
				curr = obj;
				return obj->id;
			}
			mask *= 2;
			obj++;
		}
	}
	return -1;
}

/*
	Creates a new object with obj.key=key. Object ID must be >=2.
	Must check for duplicates.

	Return value: Success --> object ID of the newly created object
				Failure --> -1
*/
long create_object(const char *key, struct objfs_state *objfs) {
		struct object *obj = objs, *free = NULL;
		int bitmap_sz = (INODE_BITMAP_CNT * BLOCK_SIZE) / 4;
		for(int i = 0; i < bitmap_sz; i++) 
			if(!free) {
				int mask = 1;
				for(int j = 0; j < NUM_BITS; j++) {
					if((INODE_BITMAP[i] & mask) == 0) {
						free = obj;
						free->id = i*32 + j + 2; // since id 0, 1 are reserved
						INODE_BITMAP[i] |= mask; // mark it filled
						break;
					}
					mask *= 2;
					obj++;
				}
			}

		if(!free){
			dprintf("%s: objstore full\n", __func__);
			return -1;
		}
		strcpy(free->key, key);

		return free->id;
}
/*
	One of the users of the object has dropped a reference
	Can be useful to implement caching.
	Return value: Success --> 0
				Failure --> -1
*/
long release_object(int objid, struct objfs_state *objfs) {
	return 0;
}

/*
	Destroys an object with obj.key=key. Object ID is ensured to be >=2.

	Return value: Success --> 0
				Failure --> -1
*/
long destroy_object(const char *key, struct objfs_state *objfs) {
	int objid = find_object_id(key, objfs);
	int idx_a = (objid - 2) / NUM_BITS;
	int idx_b = (objid - 2 - idx_a*NUM_BITS);
	struct object *curr = objs + objid - 2;
	INODE_BITMAP[idx_a] ^= 1 << idx_b; // fix inode bitmap

	for(int i = 0;i < NUM_DIRECT_BLOCKS;i++) 
		if(curr->direct[i] > 0) {
			cache_init(objfs, (curr->direct[i])%NUM_CACHE_BLOCKS);
			int idx_a = curr->direct[i]/32;
			int idx_b = curr->direct[i] - idx_a*32;
			BLOCK_BITMAP[idx_a] ^= (1 << idx_b); // fix direct block bitmap
			curr->direct[i] = 0; 
		}
	for(int i = 0;i < NUM_INDIRECT_BLOCKS;i++) 
		if(curr->indirect[i] > 0) {
			char *tmp = (char *)malloc_mp(BLOCK_SIZE);
			find_read(objfs, curr->indirect[i], tmp, BLOCK_SIZE);
			int *tmp_int = (int *)tmp;

			for(int j = 0;j < BLOCKS_IN_INDIRECT;j++) {
				int blk_no = *(tmp_int + j);
				if(blk_no > 0) {
					cache_init(objfs, blk_no%NUM_CACHE_BLOCKS);
					int idx_a = blk_no/32;
					int idx_b = blk_no - idx_a*32;
					BLOCK_BITMAP[idx_a] ^= (1 << idx_b); // fix indirect block bitmap
				}
				*(tmp_int + j) = 0;
			}
			int idx_a = curr->indirect[i]/32;
			int idx_b = curr->indirect[i] - idx_a*32;
			BLOCK_BITMAP[idx_a] ^= (1 << idx_b); // fix indirect block bitmap
			curr -> indirect[i] = 0;
			free_mp(tmp, BLOCK_SIZE);
		}
	return 0;
}

/*
	Renames a new object with obj.key=key. Object ID must be >=2.
	Must check for duplicates.  
	Return value: Success --> object ID of the newly created object
				Failure --> -1
*/

long rename_object(const char *key, const char *newname, struct objfs_state *objfs) {
	struct object *obj;
	int objid = find_object_id(key, objfs);
	obj = objs + objid - 2;
	if(strlen(newname) > 32)
		return -1;
	if(find_object_id(newname, objfs) != -1)
		return -1; // file with this key exist.
	strcpy(obj->key, newname);
	return 0;
}

/*
	Writes the content of the buffer into the object with objid = objid.
	Return value: Success --> #of bytes written
				Failure --> -1
*/

int get_free_block() {
	int bitmap_sz = (BLOCK_BITMAP_CNT * BLOCK_SIZE) / 4;
	for(int i = 0;i < bitmap_sz; i++) {
		int mask = 1;
		for(int j = 0; j < NUM_BITS; j++) {
			if((BLOCK_BITMAP[i] & mask) == 0) {
				BLOCK_BITMAP[i] |= mask;
				return (NUM_BITS*i + j);
			}
			mask *= 2;
		}
	}
	return -1;
}



long objstore_write(int objid, const char *buf, int size, struct objfs_state *objfs, off_t offset) {
	if(size > BLOCK_SIZE)
			return -1;

	// get the inode of current file
	struct object *curr = objs + objid - 2;
	dprintf("Doing write size = %d\n", size);

	int off = ((int) offset) / BLOCK_SIZE;

	if(off < 4) {
		// should work with direct pointers
		int free_blk = get_free_block();
		curr->direct[off] = free_blk;
		if(find_write(objfs, free_blk, buf, size) < 0)
			return -1;
	}
	else {
		int idx = (off - 4) / BLOCKS_IN_INDIRECT;
		if(curr->indirect[idx] == 0) {
			// need to assign this block something
			curr->indirect[idx] = get_free_block();
			int *tmp = (int *)malloc_mp(2*sizeof(int));
			int free_blk = get_free_block();
			*tmp = free_blk;
			// write to this indirect block data
			if(find_write(objfs, curr->indirect[idx], (char *)tmp, sizeof(int)) < 0) {
				return -1;
			}
			// write file data to the actual block
			if(find_write(objfs, free_blk, buf, size) < 0) {
				return -1;
			}
			free_mp(tmp, 2*sizeof(int));
		}
		else {
			// data block already exist, so we need to read the data first, and add a new entry
			char *tmp = (char *)malloc_mp(BLOCK_SIZE);
			find_read(objfs, curr->indirect[idx], tmp, BLOCK_SIZE);
			int *tmp_int = (int *)tmp;
			int pos = (off - 4) % BLOCKS_IN_INDIRECT;

			int free_blk = get_free_block();
			*(tmp_int + pos) = free_blk;
			// write this modified buffer back into indirect block
			if(find_write(objfs, curr->indirect[idx], (char *)tmp_int, BLOCK_SIZE) < 0) {
				return -1;
			}
			// write file data to the actual block
			if(find_write(objfs, free_blk, buf, size) < 0) {
				return -1;
			}
			free_mp(tmp, BLOCK_SIZE);
		}
	}
	curr -> size += size;
	return size;
}


/*
	Reads the content of the object onto the buffer with objid = objid.
	Return value: Success --> #of bytes written
				Failure --> -1
*/
long objstore_read(int objid, char *buf, int size, struct objfs_state *objfs, off_t offset) {
	if(objid < 2)
			return -1;
	struct object *curr = objs + objid - 2;

	int off_blk_cnt = ((int) offset) / BLOCK_SIZE;
	dprintf("Doing read size = %d\n", size);

	int sz = size / BLOCK_SIZE, sz_a;
	if(size % BLOCK_SIZE != 0)
		sz++;
	for(int i = 0;i < sz;i++) {
		if(i != sz - 1)
			sz_a = BLOCK_SIZE;
		else
			sz_a = size - BLOCK_SIZE*i;
		int off = off_blk_cnt + i;
		if(off < 4) {
			// should read directly
			if(find_read(objfs, curr->direct[off], buf + i*BLOCK_SIZE, sz_a) < 0)
				return -1;
		}
		else {
			int idx = (off - 4) / BLOCKS_IN_INDIRECT;
			int pos = (off - 4) % BLOCKS_IN_INDIRECT;
			char *tmp = (char *)malloc_mp(BLOCK_SIZE);
			
			find_read(objfs, curr->indirect[idx], tmp, BLOCK_SIZE);
			
			int *tmp_int = (int *)tmp;
			int blk_id = *(tmp_int + pos);

			if(find_read(objfs, blk_id, buf + i*BLOCK_SIZE, sz_a) < 0)
				return -1;
			free_mp(tmp, BLOCK_SIZE);
		}
	}

	return size;
}

/*
	Reads the object metadata for obj->id = buf->st_ino
	Fillup buf->st_size and buf->st_blocks correctly
	See man 2 stat 
*/
int fillup_size_details(struct stat *buf, struct objfs_state *objfs) {
	struct object *tmp =  objs + buf->st_ino - 2;
	if(buf->st_ino < 2 || tmp->id != buf->st_ino)
			return -1;
	buf->st_size = tmp->size;
	buf->st_blocks = tmp->size >> 9;
	if(((tmp->size >> 9) << 9) != tmp->size)
			buf->st_blocks++;
	return 0;
}

/*
	Set your private pointeri, anyway you like.
*/
int objstore_init(struct objfs_state *objfs) {
	
	struct stat sbuf;
	if(fstat(objfs->blkdev, &sbuf) < 0){
		perror("fstat");
		exit(-1);
	}
	NUM_BLOCKS = sbuf.st_size / BLOCK_SIZE;

	// copy inode bitmap
	void *tmp = NULL;
	tmp = malloc_mp(INODE_BITMAP_CNT * BLOCK_SIZE);
	for(int i = INODE_BITMAP_START; i <= INODE_BITMAP_END; i++) {
		if(read_block(objfs, i, (char *)(tmp + BLOCK_SIZE*(i - INODE_BITMAP_START))) < 0)
			return -1;
	}
	INODE_BITMAP = (int *)tmp;
	
	// copy block bitmap data
	tmp = NULL;
	tmp = malloc_mp(BLOCK_BITMAP_CNT * BLOCK_SIZE);
	for(int i = BLOCK_BITMAP_START; i <= BLOCK_BITMAP_END; i++) {
		if(read_block(objfs, i, (char *)(tmp + BLOCK_SIZE*(i - BLOCK_BITMAP_START))) < 0)
			return -1;
	}
	BLOCK_BITMAP = (int *)tmp;

	// copy inodes in memory
	tmp = NULL;
	tmp = malloc_mp(INODE_CNT * BLOCK_SIZE);
	for(int i = INODE_START; i <= INODE_END; i++) {
		if(read_block(objfs, i, (char *)(tmp + BLOCK_SIZE*(i - INODE_START))) < 0) {
			return -1;
		}
	}

	objs = (struct object *)tmp;
	objfs->objstore_data = (void *)objs;
	
	// set all these block bit to one
	for(int i = 0;i <= INODE_END;i++) {
		int idx_a = i/32;
		int idx_b = i - idx_a*32;
		BLOCK_BITMAP[idx_a] |= (1 << idx_b);
	}

	dprintf("Done objstore init\n");
	return 0;
}

/*
	Cleanup private data. FS is being unmounted
*/
int objstore_destroy(struct objfs_state *objfs) {

	// copy inode bitmap into disk
	void *tmp = INODE_BITMAP;
	for(int i = INODE_BITMAP_START; i <= INODE_BITMAP_END; i++) {
		if(write_block(objfs, i, (char *)(tmp + BLOCK_SIZE*(i - INODE_BITMAP_START))) < 0)
			return -1;
	}
	free_mp((void *)INODE_BITMAP, INODE_BITMAP_CNT * BLOCK_SIZE);
	INODE_BITMAP = NULL;
	
	// copy block bitmap data to disk
	tmp = BLOCK_BITMAP;
	for(int i = BLOCK_BITMAP_START; i <= BLOCK_BITMAP_END; i++) {
		if(write_block(objfs, i, (char *)(tmp + BLOCK_SIZE*(i - BLOCK_BITMAP_START))) < 0)
			return -1;
	}
	free_mp((void *)BLOCK_BITMAP, BLOCK_BITMAP_CNT * BLOCK_SIZE);
	BLOCK_BITMAP = NULL;

	// copy inodes to disk
	tmp = (void *)objs;
	for(int i = INODE_START; i <= INODE_END; i++) {
		if(write_block(objfs, i, (char *)(tmp + BLOCK_SIZE*(i - INODE_START))) < 0) {
			return -1;
		}
	}
	for(int i = 0;i < NUM_CACHE_BLOCKS;i++) {
		struct cache_struct *cache = ((struct cache_struct *)objfs -> cache) + i;
		if(cache->dirty)
			dprintf("%s\n", cache->data);
		obj_sync(objfs, i);
		cache_init(objfs, i);
	}
	free_mp((void *)objs, INODE_CNT * BLOCK_SIZE);
	objfs->objstore_data = NULL;
	dprintf("Done objstore destroy\n");
	return 0;
}
