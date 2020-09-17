////
// @file table.cc
// @brief
// 实现存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/table.h>
#include<db/schema.h>
#include<db/block.h>
#include<db/record.h>
#include<iostream>
using namespace db;

int key;
char *type;
DataType *dt;
char *name;
File x;
const char *table1=nullptr;


void readTable(const char *table);
int initFile(); 

namespace db 
{

int GetOffset(int blockid);
int iovecCmp(iovec* a, iovec* b);

//从大到小
bool cmp(std::pair<iovec*, int> a, std::pair<iovec*, int> b)
{
	iovec* p = &a.first[key];
	iovec* q = &b.first[key];
	return iovecCmp(p, q) > 0;
}

//从小到大
bool cmp1(iovec* a, iovec* b)
{
	iovec* p = &a[key];
	iovec* q = &b[key];
	return iovecCmp(p, q) < 0;
}


int create(const char *name, RelationInfo &info)
{
    return gschema.create(name, info);
}

int Table::open(const char *name)
{
    // 查找schema
    std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup(name);
    if (!ret.second) return EINVAL;
    // 找到后，加载meta信息
    return gschema.load(ret.first);
}

//先依据索引往下搜索，直到搜索到叶结点
//搜索到应该插入的blockid后，插入对应的datablock
//若block有分裂，则向上调整索引
int Table::insert(struct iovec *record, size_t iovcnt,const char* table)
{
    readTable(table);
    unsigned long long len = initFile();
    
	block_iterator p(table);
	IndexBlock ib(table);
	if (ib.getSlotsNum() == 0)
	{
		//第一个indexblock还没有记录，说明datablock还没有分裂过
		if (p.canInsert(record, iovcnt))
		{
			p.insert(record, iovcnt);
			p.BlockSort();
			p.write();
			return S_OK;
		}
		else
		{
			p.Spilt(record, iovcnt,table);
		}
	}
	else
	{
		int id=ib.GetBlockId(&record[key]);
		p.jump(id);
		if (p.canInsert(record, iovcnt))
		{
			p.insert(record, iovcnt);
			p.BlockSort();
			p.write();
		}
		else
		{

			p.Spilt(record, iovcnt,table);		
		}
	}
	return S_OK;	
}


int Table::remove(iovec *keyinf,const char* table) 
{
    readTable(table);
    unsigned long long len = initFile();
	IndexBlock ib(table);

	int id = ib.GetBlockId(keyinf);
	block_iterator p(table);
	p.jump(id);
	p.remove(keyinf);

	

	return 0;
}

int Table::update(iovec *keyinfo, iovec *info,int fieldKey, const char *table)
{
	gschema.open();
	std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup(table);
	int num = ret.first->second.count+1;


	IndexBlock ib(table);
	iovec* q = ib.Inrequire(keyinfo);

	if (q == nullptr)
		return 0;

	iovec* p = new iovec[num];



	for (int i = 0; i < num; i++)
	{
		if (i == fieldKey)
		{
			p[i].iov_base = (void*)(new char[info->iov_len]);
			memcpy(p[i].iov_base,info->iov_base, info->iov_len);
			p[i].iov_len = info->iov_len;
			continue;
		}
		p[i].iov_base = (void*)(new char[q[i].iov_len]);
		memcpy(p[i].iov_base,q[i].iov_base,q[i].iov_len);
		p[i].iov_len = q[i].iov_len;
	}

	remove(keyinfo, table);
	insert(p, num, table);
	return 0;
}


iovec *Table::inquire(iovec *key, char *table) 
{ 
	IndexBlock ib(table);
	return ib.Inrequire(key);
}

//获取某个block的偏移量
int GetOffset(int blockid)
{
	return Root::ROOT_SIZE + (blockid - 1) * Block::BLOCK_SIZE;
}

////
//参数:a,b=要比较的两个iovec
//作用:比较两个iovec(一般是key)大小
//返回值:>0为a>b，<0为a<b ,=0为a=b (字母大于数字)
////
int iovecCmp(iovec* a, iovec* b)
{
	int result = memcmp(a->iov_base, b->iov_base, a->iov_len);
	//先判断是否相等
	if (result == 0 && a->iov_len == b->iov_len)
		return 0;
	else
		return result;
	//再判断大小        
}


// nextid
int block_iterator::nextid() { return db.getNextid(); }

//判断是否能插入
bool block_iterator::canInsert(iovec* iov, int iovcnt)
{
	// 判断是否有空间
	unsigned short length = db.getFreeLength();
	if (length == 0) return false;
	// 判断能否分配
	std::pair<size_t, size_t> ret = Record::size(iov, iovcnt);
	length -= 2; // 一个slot占2字节
	if (ret.first > length) return false;
	return true;
}

//直接插入
bool block_iterator::insert(iovec* iov, int iovcnt)
{
	unsigned short length = db.getFreeLength();
	// 写入记录
	Record record;
	unsigned short oldf = db.getFreespace();
	record.attach(buffer + oldf, length);
	unsigned char header = Record::initHeader(false, false);
	unsigned short pos = (unsigned short)record.set(iov, iovcnt, &header);

	// 调整freespace
	db.setFreespace(pos + oldf);
	// 写slot
	unsigned short slots = db.getSlotsNum();
	db.setSlotsNum(slots + 1); // 增加slots数目
	db.setSlot(slots, oldf);   // 第slots个
	return true;
}

//下个block
void block_iterator::operator++()
{
	if (db.getNextid() != -1) blockid = db.getNextid();
	x.read(GetOffset(blockid), (char*)buffer, Block::BLOCK_SIZE);
	db.attach(buffer);
}

//跳转至某个block
void block_iterator::jump(int blockid)
{	
	this->blockid = blockid;
	unsigned char midbuf[Block::BLOCK_SIZE];
	x.read(GetOffset(blockid), (char*)midbuf, Block::BLOCK_SIZE);
	memcpy(buffer, midbuf, Block::BLOCK_SIZE);
	db.attach(buffer);
}

//将当前block写入磁盘
void block_iterator::write()
{
	x.write(GetOffset(db.blockid()), (char*)buffer, Block::BLOCK_SIZE);
}

unsigned char* block_iterator::GetNewBlock()
{
	DataBlock b;
	unsigned char* nextblock = new unsigned char[Block::BLOCK_SIZE];
	b.attach(nextblock);
	unsigned long long len;
	x.length(len);
	x.read(
		GetOffset(root.getGarbage()),
		(char*)nextblock,
		Block::BLOCK_SIZE);


	//如果是文件尾部的block
	if (len == GetOffset(root.getGarbage()))
	{
		b.clear(root.getGarbage(), 0);
		root.setGarbage(root.getGarbage() + 1);
	}
	else
	{
		b.clear(b.blockid(), 0);
		root.setGarbage(b.getGarbage());
	}
	next.push_back(nextblock);
	x.write(GetOffset(b.blockid()),(char*)nextblock,Block::BLOCK_SIZE);
	return nextblock;
}

void block_iterator::BlockSort()
{
	//获取需要的key和type
	int slotNum = db.getSlotsNum();
	std::vector<int> offset;
	std::vector<std::pair<iovec*, int>> s;
	for (int i = 0; i < slotNum; i++) {
		offset.push_back(db.getSlot(i));
	}
	iovec** p = new iovec * [slotNum];
	Record* q = new Record[slotNum];
	Integer it;

	for (int i = 0; i < slotNum; i++) {
		it.decode((char*)buffer + offset[i], 8);
		q[i].attach(buffer + offset[i], it.get());

		//如果这个记录没有被删除
		if (!q[i].is_delete())
		{
			p[i] = q[i].getKey(key);
			std::pair<iovec*, int> a;
			a.first = p[i];
			a.second = offset[i];
			s.push_back(a);
		}
	}

	std::sort(s.begin(), s.end(), cmp);

	for (int i = 0; i < slotNum; i++) 
	{
		db.setSlot(i, s[i].second);
		delete s[i].first;
	}
	db.setChecksum();
	s.clear();
	delete[] p;
	delete[] q;
}

void block_iterator::Spilt(iovec* record, int iovcnt,const char* table)
{
	std::vector<iovec*>allRecord;
	db.getAllRecord(allRecord);
	allRecord.push_back(record);
	std::sort(allRecord.begin(), allRecord.end(), cmp1);

	int nextid = db.getNextid();
	unsigned char* buf = buffer;
	unsigned char midbuf[Block::BLOCK_SIZE];
	unsigned char midbuf2[Block::BLOCK_SIZE];

	memcpy(midbuf, buffer, Block::BLOCK_SIZE);
	memcpy(midbuf2, buffer, Block::BLOCK_SIZE);
	db.attach(midbuf);
	db.clear(db.blockid(), 0);
	db.attach(midbuf2);
	db.clear(db.blockid(), 0);

	db.attach(midbuf);
	buffer = midbuf;

	DataBlock nextblock;
	nextblock.attach(GetNewBlock());
	db.setNextid(nextblock.blockid());
	IndexBlock ib(table);

	//现在的block写一半
	for (int i = 0; i < allRecord.size() / 2; i++)
	{
		insert(allRecord[i], iovcnt);
	}

	Record rd;
	rd.attach(midbuf + db.getSlot(0));

	BlockSort();
	x.write(GetOffset(db.blockid()), (char*)midbuf, Block::BLOCK_SIZE);
	x.read(GetOffset(db.blockid()), (char*)midbuf, Block::BLOCK_SIZE);	
	ib.InsertRecord(&allRecord[allRecord.size() / 2-1][key], db.blockid());

	buffer = midbuf2;
	jump(nextblock.blockid());
	db.setNextid(nextid);

	for (int i = allRecord.size() / 2; i < allRecord.size(); i++)
	{
		insert(allRecord[i], iovcnt);
	}
	ib.InsertRecord(&allRecord[allRecord.size() - 1][key], db.blockid());
	BlockSort();
	x.write(GetOffset(db.blockid()), (char*)midbuf2, Block::BLOCK_SIZE);
	buffer = buf;
}

bool block_iterator::remove(iovec* keyinf)
{
	readTable(table);
	Record rd;
	for (int i = 0; i < db.getSlotsNum(); i++)
	{
		rd.attach(buffer + db.getSlot(i));
		iovec* q = rd.getKey(key);
		if (iovecCmp(keyinf, q) == 0)
		{
			//如果datablock要被删光了的话，还要修改Index
			if (db.getSlotsNum() == 1)
			{
				IndexBlock ib(table);
				ib.DeleteRecord(keyinf);
			}
			rd.setheader(Record::initHeader(false,true));
			db.clearGarbage(this->x);
			return true;
		}
		delete[]q;
	}
	return false;
}




} // namespace db




    //读取表信息
    void readTable(const char *table) 
    {
        if (table1==nullptr || !memcmp(table, table1, strlen(table))) { //先获取表信息
            gschema.open();
            std::pair<Schema::TableSpace::iterator, bool> tb =gschema.lookup(table);
            //获取需要的key和type
            key = tb.first->second.key;
            type = (char *) tb.first->second.fields[key].type.c_str();
            dt = findDataType(type);
            name = (char *) dt->name;
            table1 = table;
            std::string a(table);
            a=a + ".db";
            x.open(a.c_str());
        }        
    }

    

    int initFile() 
    {
        unsigned long long len = 0;
        x.length(len);
        if (!len) {
            Root root;
            unsigned char rb[Root::ROOT_SIZE];
            root.attach(rb);
            root.clear(BLOCK_TYPE_DATA);
            root.setHead(1);
            root.setGarbage(2); // garbage默认是文件尾的空block
            // 创建第1个block
            DataBlock block;
            unsigned char *buffer_ = new unsigned char[Block::BLOCK_SIZE];
            block.attach(buffer_);
            block.clear(1, 0);
            // 写root和block
            x.write(0, (const char *) rb, Root::ROOT_SIZE);
            x.write(Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
            delete[] buffer_;
        } 
        return len;
    }