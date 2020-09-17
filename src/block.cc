////
// @file block.cc
// @brief
// 实现block
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/block.h>
#include <db/record.h>
#include<db/schema.h>
#include<vector>
#include<db/config.h>
#include<db/datatype.h>
#include<algorithm>
#include<iostream>

bool (*f)(const void *, const void *, size_t, size_t);
bool cmp(std::pair<iovec *, int> a, std::pair<iovec *, int> b)
{
    return f(
        a.first->iov_base,
        b.first->iov_base,
        a.first->iov_len,
        b.first->iov_len);
}





namespace db {

	extern int GetOffset(int blockid);
	extern int iovecCmp(iovec* a, iovec* b);

	bool Block::LargerThanBlock(iovec* key)
	{
		if (getSlotsNum() == 0)
			return false;
		else
		{
			Record rd;
			rd.attach(buffer_ + getSlot(0));
			iovec* p = rd.getKey(0);
			if (iovecCmp(key, p) > 0)
				return true;
			else
				return false;
		}
	}

void Block::clear(int spaceid, int blockid)
{
    spaceid = htobe32(spaceid);
    blockid = htobe32(blockid);
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + BLOCK_MAGIC_OFFSET, &BLOCK_MAGIC_NUMBER, BLOCK_MAGIC_SIZE);
    // 设置spaceid
    ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
    // 设置blockid
    ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
    // 设置freespace
    unsigned short data = htobe16(BLOCK_DEFAULT_FREESPACE);
    ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);

	setNextid(-1);
	// 设置checksum
	setChecksum();
}

void Root::clear(unsigned short type)
{
    // 清buffer
    ::memset(buffer_, 0, ROOT_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + Block::BLOCK_MAGIC_OFFSET,
        &Block::BLOCK_MAGIC_NUMBER,
        Block::BLOCK_MAGIC_SIZE);
    // 设定类型
    setType(type);
    // 设定时戳
    TimeStamp ts;
    ts.now();
    setTimeStamp(ts);
    // 设置checksum
    setChecksum();
}

void MetaBlock::clear(unsigned int blockid)
{
    unsigned int spaceid = 0xffffffff; // -1表示meta
    blockid = htobe32(blockid);
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + BLOCK_MAGIC_OFFSET, &BLOCK_MAGIC_NUMBER, BLOCK_MAGIC_SIZE);
    // 设置spaceid
    ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
    // 设置blockid
    ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
    // 设置freespace
    unsigned short data = htobe16(META_DEFAULT_FREESPACE);
    ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);
    // 设定类型
    setType(BLOCK_TYPE_META);
    // 设置checksum
    setChecksum();
}

bool Block::allocate(const unsigned char *header, struct iovec *iov, int iovcnt)
{
    // 判断是否有空间
    unsigned short length = getFreeLength();
    if (length == 0) return false;
    std::pair<size_t, size_t> ret = Record::size(iov, iovcnt);
    length -= 2; // 一个slot占2字节
    if (ret.first > length) return false;

    // 写入记录
    Record record;
    unsigned short oldf = getFreespace();
    record.attach(buffer_ + oldf, length);
    unsigned short pos = (unsigned short) record.set(iov, iovcnt, header);

    // 调整freespace
    setFreespace(pos + oldf);
    // 写slot
    unsigned short slots = getSlotsNum();
    setSlotsNum(slots + 1); // 增加slots数目
    setSlot(slots, oldf);   // 第slots个
    
    return true;
}



    void DataBlock::clear(unsigned int blockid,unsigned int spaceid) 
    {
        blockid = htobe32(blockid);
        spaceid = htobe32(spaceid);
        // 清buffer
        ::memset(buffer_, 0, BLOCK_SIZE);
        // 设置magic number
        ::memcpy(
            buffer_ + BLOCK_MAGIC_OFFSET,
            &BLOCK_MAGIC_NUMBER,
            BLOCK_MAGIC_SIZE);
        // 设置spaceid
        ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
        // 设置blockid
        ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
        // 设置freespace
        unsigned short data = htobe16(Block::BLOCK_DEFAULT_FREESPACE);
        ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);
        // 设定类型
        setType(BLOCK_TYPE_DATA);
        // 设置checksum
        setChecksum();
        //-1表示空
        setNextid(-1);
        setGarbage(0);
    }


    bool DataBlock::allocate(const unsigned char *header,struct iovec *iov,int iovcnt)
    {
        if (!canInsert(iov, iovcnt)) return false;
        unsigned short length = getFreeLength();
        // 写入记录
        Record record;
        unsigned short oldf = getFreespace();
        record.attach(buffer_ + oldf, length);
        unsigned short pos = (unsigned short) record.set(iov, iovcnt, header);

        // 调整freespace
        setFreespace(pos + oldf);
        // 写slot
        unsigned short slots = getSlotsNum();
        setSlotsNum(slots + 1); // 增加slots数目
        setSlot(slots, oldf);   // 第slots个
        return true;    
    }

    void DataBlock::BlockSort(int key, char *type, int fileoffset, File &x) 
    {
        int slotNum = getSlotsNum();
        std::vector<int> offset;
        std::vector<std::pair<iovec *, int> > s;
        for (int i = 0; i < slotNum; i++) {
            offset.push_back(getSlot(i));
        }
        iovec **p = new iovec *[slotNum];
        Record *q = new Record[slotNum];
        Integer it;

        for (int i = 0; i < slotNum; i++) {
            it.decode((char *) buffer_ + offset[i], 8);
            q[i].attach(buffer_ + offset[i], it.get());

            //如果这个记录没有被删除
            if (!q[i].is_delete()) 
            {   
                p[i] = q[i].getKey(key); 
                std::pair<iovec *, int> a;
                a.first = p[i];
                a.second = offset[i];
                s.push_back(a);
            }
        }

        DataType *dt = findDataType(type);
        auto cmp1 = dt->compare;
        f = cmp1;
        std::sort(s.begin(), s.end(), cmp);

        for (int i = 0; i < slotNum; i++) 
        {
            setSlot(i, s[i].second);
            delete s[i].first;
        }
        setChecksum();
        s.clear();

        x.write(fileoffset, (const char*)buffer_, DataBlock::BLOCK_SIZE);
        delete[] p;
        delete[] q;
    }



    void Block::clearGarbage(File&x) 
    { 
        int num = getSlotsNum();
        Integer it;
        int offset = BLOCK_DEFAULT_FREESPACE;
        
        std::vector<iovec*> allRecord;//block内未被删除的记录
        unsigned char *header = new unsigned char;
        Record record;
        int iovcnt;
        for (int i = 0; i < num; i++) 
        {
            offset = getSlot(i);
            it.decode((char*)buffer_ + offset, 8);
            record.attach(buffer_ + offset, it.get());
            
            if (!record.is_delete()) 
            {                 
                iovcnt = record.fields();
                iovec *p = new iovec[iovcnt];
                record.ref(p, record.fields(),header);

                std::string x((char *) p[0].iov_base);
                allRecord.push_back(p);               
            }
        }
        
        if (allRecord.size() == num) 
        {
            for (int i = 0; i < allRecord.size(); i++) 
            {
                delete allRecord[i];
            }
            delete header;
            return; //如果没有记录删除，就不用操作了
        }

        unsigned char *buf = new unsigned char[Block::BLOCK_SIZE];
        memcpy(buf,buffer_,Block::BLOCK_SIZE);

        int nextid = getNextid();
        attach(buf);
        clear(0, blockid());
        setNextid(nextid);
        if (allRecord.size() == 0) 
        { 
            //如果记录删完了，加入链表
            Root root;
            unsigned char *rtbuf = new unsigned char[Root::ROOT_SIZE];
            x.read(0, (char *) rtbuf, Root::ROOT_SIZE);
            root.attach(rtbuf);
                 

            if (blockid() == root.getHead()) 
            { 
				if (getNextid() == -1)
				{
					x.write(GetOffset(blockid()), (char*)buf, Block::BLOCK_SIZE);
					return;
				}
				root.setHead(getNextid());
				setGarbage(root.getGarbage());
				root.setGarbage(blockid());
                x.write(GetOffset(blockid()),(char*)buf,Block::BLOCK_SIZE);
				x.write(0, (char*)rtbuf, Root::ROOT_SIZE);
                return;
            }

            //找到当前block的前一个block,并调整指针.
            Block db;
            unsigned char *buffer = new unsigned char[Block::BLOCK_SIZE];
            db.attach(buffer);
            x.read(GetOffset(root.getHead()), (char *) buffer, Block::BLOCK_SIZE);

            while (db.getNextid() != blockid()) 
            {
                x.read(GetOffset(db.getNextid()), (char *) buffer, Block::BLOCK_SIZE);                      
            }
            db.setNextid(getNextid());
            x.write(GetOffset(db.blockid()),(char*)buffer,Block::BLOCK_SIZE);

            setGarbage(root.getGarbage());
            root.setGarbage(blockid());
            x.write(0, (char *) rtbuf, Root::ROOT_SIZE);
			x.write(GetOffset(blockid()), (char*)buf, Block::BLOCK_SIZE);
            delete[] buffer;
            delete[] buf;
			return;
        }

        for (int i = 0; i < allRecord.size(); i++) 
        {
            allocate(header, allRecord[i], iovcnt); //由于原block有序，所以删掉一些记录后，依然有序，不用排序   
            delete allRecord[i];
        }
        x.write(GetOffset(blockid()),(char*)buf,BLOCK_SIZE);
        delete header;
        delete[] buf;
    }

	void Block::getAllRecord(std::vector<iovec*>&allRecord)
	{
		Record rd;
		int num;
		rd.attach(buffer_ + getSlot(0));
		num = rd.fields();
		unsigned char header = ' ';
		for (int i = 0; i < getSlotsNum(); i++)
		{			
			rd.attach(buffer_ + getSlot(i));
			iovec* p = new iovec[num];
			rd.ref(p,num,&header);
			allRecord.push_back(p);
		}	
	}
} // namespace db

