////
// @file btree.cc
// @brief
// 实现b+树存储索引
//
// @author wzy
// @email uestcwzyyyyy@gmail.com
//

#include "db/btree.h"
#include <vector>
#include <algorithm>
#include "db/block.h"

namespace db {
extern int GetOffset(int blockid);
extern int iovecCmp(iovec *a, iovec *b);

bool keyCmp(iovec* a, iovec* b)
{
	int result = memcmp(a[0].iov_base, b[0].iov_base, a[0].iov_len);
	//先判断是否相等
	return result>0;
	//再判断大小       
}

IndexBlock::IndexBlock()
{
	buffer_ = nullptr;
	rootbuffer == nullptr;
}

bool IndexBlock::isLeaf()
{
	if (getSlotsNum() == 0) return true;
	Integer it;
	int x = getSlot(0);
	unsigned char* buf = buffer_ + x;
	it.decode((char*)buf, 8);

	//根据第一个记录的head确定
	Record rd;
	rd.attach(buf, it.get());
	return rd.IsOverBlock();
}




IndexBlock::IndexBlock(const char *table)
{
    this->table = table;
    std::string a((char *) table);
    x.open((a + ".index").c_str());
    unsigned long long len = initFile(); //若文件还不存在则初始化文件
	
    buffer_ = new unsigned char[Block::BLOCK_SIZE];
	attach(buffer_);

    rootbuffer=new unsigned char[Root::ROOT_SIZE];
	root.attach(rootbuffer);

    x.read(0, (char *) rootbuffer, Root::ROOT_SIZE);
    x.read(
        GetOffset(root.getHead()),
        (char *) buffer_,
        Block::BLOCK_SIZE); //读入根结点
}

unsigned long long IndexBlock::initFile()
{
    unsigned long long len = 0;
    x.length(len);
    if (!len) {
        Root root;
        unsigned char *rb=new unsigned char[Root::ROOT_SIZE];
        root.attach(rb);
        root.clear(BLOCK_TYPE_INDEX);
        root.setHead(1);
        root.setGarbage(2); // garbage默认是文件尾的空block
        // 创建第1个block
        unsigned char *buffer = new unsigned char[Block::BLOCK_SIZE];
		DataBlock ib;
		ib.attach(buffer);
		ib.clear(1,0);
		ib.setNextid(-1);
        x.write(0, (char *) rb, Root::ROOT_SIZE);
        x.write(Root::ROOT_SIZE, (const char *) buffer, Block::BLOCK_SIZE);
    }
    return len;
}


int IndexBlock::GetBlockId(iovec *key)
{
    if (buffer_ == nullptr) return S_FALSE;
    Root root;
    unsigned char rootbuffer[Root::ROOT_SIZE];
    root.attach(rootbuffer);
    x.read(0, (char *) rootbuffer, Root::ROOT_SIZE);
    x.read(GetOffset(root.getHead()), (char *) buffer_, Block::BLOCK_SIZE);

    Record rd;
    while (!isLeaf()) 
	{
        int pos = FindPlace(key);
        rd.attach(buffer_ + getSlot(pos));
        iovec *q = rd.getKey(1);
        x.read(
            GetOffset(*(int *) q->iov_base),
            (char *) buffer_,
            Block::BLOCK_SIZE);
    }

    int pos = FindPlace(key);
    rd.attach(buffer_ + getSlot(pos));
    iovec *q = rd.getKey(1);
    return *(int *) q->iov_base;
}

//插入索引
void IndexBlock::InsertRecord(iovec *key, int dataid)
{
	//非最大key所在的block内，由于插入的值总是小于key的，所以分裂后必会产生一个以原来key为key的记录，这种只需要修改指针。产生的另外一个记录直接插入即可
	//最大key所在的block内，插入的值可能大于key,所以先要比较其和最大key的大小，如果指针都指向最大key,但key不同，则修改key。不指向同一个的话，直接插入

    while (!isLeaf()) 
	{
		path.push_back(blockid());
        if (LargerThanBlock(key)) 
		{
            x.read(GetOffset(getNextid()), (char *) buffer_, Block::BLOCK_SIZE);
        } 
		else 
		{
			int pos = getSlot(FindPlace(key));
			Record rd;
			Integer it;
			unsigned char* buf = buffer_ + pos;
			it.decode((char*)buf, 8);
			rd.attach(buf, it.get());
			iovec* q = rd.getKey(1);
			int nextid = *(int*)q->iov_base; // key=1表示取指针
			x.read(GetOffset(nextid), (char*)buffer_, Block::BLOCK_SIZE);
			delete[] q;
        }
    }

	//插入block内
	iovec q[2];
	q[0].iov_base = key->iov_base;
	q[0].iov_len = key->iov_len;
	q[1].iov_base = &dataid;
	q[1].iov_len = sizeof(int);

	////如果比最大的都大的话
	//if (LargerThanBlock(key))
	//{
	//	Record rd;
	//	rd.attach(buffer_ + getSlot(0));
	//	iovec p[2];
	//	unsigned char header = ' ';
	//	rd.ref(p, 2, &header);
	//	//如果指向的是同一个block，则修改key(相当于删掉原来的插入q)
	//	if (*(int*)p[1].iov_base == *(int*)q[1].iov_base)
	//	{
	//		rd.setheader(Record::initHeader(true,true));
	//		clearGarbage();
	//		allocate(&header, q, 2);
	//		BlockSort();
	//		write();
	//		return;
	//	}
	//	//不指向同一block,等待下一步直接插入
	//}

	unsigned char header = Record::initHeader(true,false);
	if (getSlotsNum() == 0)
	{
		allocate(&header, q, 2);
		BlockSort();
		write();
		return;
	}

	Record rd;
	rd.attach(buffer_ + getSlot(0));
	iovec p[2];
	rd.ref(p, 2, &header);
	//如果指向的是同一个block，则修改key(相当于删掉原来的插入q)
	if (*(int*)p[1].iov_base == dataid)
	{
		rd.setheader(Record::initHeader(true, true));
		clearGarbage();
		allocate(&header, q, 2);
		BlockSort();
		write();
		return;
	}
	

	//如果有一样的key的话，修改指针
	for (int i = 0; i < getSlotsNum(); i++)
	{
		Record rd;
		rd.attach(buffer_ + getSlot(i));
		iovec* xx = rd.getKey(0);
		if (iovecCmp(xx, key) == 0)
		{
			rd.setheader(Record::initHeader(true, true));
			clearGarbage();
			allocate(&header, q, 2);
			BlockSort();
			write();
			return;
		}
		delete[]xx;
	}

	if (canInsert(q, 2))
	{
		unsigned char header = Record::initHeader(true, false);
		allocate(&header, q, 2);
		BlockSort();
		write();
	}
	else
	{
		Spilt(q);
	}
	
}

int IndexBlock::FindPlace(iovec *key)
{
    if (getSlotsNum() <= 1) { return 0; }
	if (LargerThanBlock(key)) return 0;
    int head = 0;
    int tail1 = getSlotsNum() - 1;
    iovec *rdkey;
    Record rd;
    Integer it;

	//如果比第二个key大的话，直接返回最大key的位置---0
	unsigned char* buf = buffer_ + getSlot(1);
	it.decode((char*)buf, 8);
	rd.attach(buf, it.get());
	rdkey = rd.getKey(0);
	int x = iovecCmp(rdkey, key);
	if (x < 0)
		return 0;

	//如果比最小的还小，直接返回最小key位置
	buf = buffer_ + getSlot(getSlotsNum()-1);
	it.decode((char*)buf, 8);
	rd.attach(buf, it.get());
	rdkey = rd.getKey(0);
	x = iovecCmp(rdkey, key);
	if (x >0)
		return getSlotsNum()-1;


    while (1) 
	{
        int pos = (head + tail1) / 2;
        unsigned char *buf = buffer_ + getSlot(pos);
        it.decode((char *) buf, 8);
        rd.attach(buf, it.get());
        rdkey = rd.getKey(0);
        int x = iovecCmp(rdkey, key);
        if (x == 0) return pos;

        if (x > 0)
			head = pos;
        else
            tail1 = pos;

		if (head >= tail1 - 1)
		{
				return head;
		}
    }
}

//key是传上来的整个记录,iovcnt=2
void IndexBlock::Spilt(iovec* key)
{
	int height = path.size()-1;
	std::vector<iovec*>allRecord;
	getAllRecord(allRecord);
	allRecord.push_back(key);
	std::sort(allRecord.begin(),allRecord.end(),keyCmp);

	//先处理叶结点
	int nextid = getNextid();
	IndexBlock ib;
	unsigned char* nextbuffer = GetNewBlock();
	ib.attach(nextbuffer);
	unsigned char midbuf[Block::BLOCK_SIZE];
	memcpy(midbuf, buffer_, Block::BLOCK_SIZE);
	unsigned char* buf = buffer_;

	attach(midbuf);
	clear(0, blockid());

	unsigned char header = Record::initHeader(true, false);

	for (int i = 0; i <= allRecord.size() / 2; i++)
	{
		allocate(&header, allRecord[i], 2);
		delete[]allRecord[i];
	}

	for (int i = allRecord.size() / 2 + 1; i <= allRecord.size(); i++)
	{
		ib.allocate(&header, allRecord[i], 2);
		delete[]allRecord[i];
	}

	setNextid(ib.blockid());
	ib.setNextid(nextid);
	memcpy(buffer_, midbuf, BLOCK_SIZE);
	write();
	ib.write(x);
	
	//处理叶结点后往上传逐层处理
	int id = ib.blockid();
	iovec q[2];
	q[0].iov_base = allRecord[allRecord.size() / 2]->iov_base;
	q[0].iov_len = allRecord[allRecord.size() / 2]->iov_len;
	q[1].iov_base = &id;
	q[1].iov_len = sizeof(int);
	bool up = true;

	while (height>=0&&up)
	{
		//不是末尾结点分裂产生的记录不可能比末尾结点大(因为其所有的值都比末尾结点小)，所以正常插入即可
		//末尾结点分裂产生的记录，因为原指针指向的是原结点，由于新节点的所有值都比原结点大，所以上传原来的key和新的blockid，将原指针和key组合后插入，并将nextblock改为新的
		x.read(GetOffset(path[height]), (char*)buffer_, BLOCK_SIZE);
		up = false;

		if (canInsert(q, 2))
		{
			unsigned char header = Record::initHeader(false, false);
			int i = getNextid();
			setNextid(*(int*)q[1].iov_base);
			q[1].iov_base = &i;
			allocate(&header, q, 2);		
		}
		else
		{
			//非叶结点的分裂
			//和叶结点相似，只不过中间的值不插入结点中，直接给上一层
			std::vector<iovec*>allRecord;
			getAllRecord(allRecord);
			allRecord.push_back(q);
			std::sort(allRecord.begin(), allRecord.end(), keyCmp);
			int nextid = getNextid();

			IndexBlock ib;
			unsigned char* nextbuffer = GetNewBlock();
			ib.attach(nextbuffer);
			unsigned char midbuf[Block::BLOCK_SIZE];
			memcpy(midbuf, buffer_, Block::BLOCK_SIZE);
			unsigned char* buf = buffer_;

			attach(midbuf);
			clear(0, blockid());

			unsigned char header = Record::initHeader(true, false);

			for (int i = 0; i < allRecord.size() / 2; i++)
			{
				allocate(&header, allRecord[i], 2);
				delete[]allRecord[i];
			}

			for (int i = allRecord.size() / 2 + 1; i <= allRecord.size(); i++)
			{
				ib.allocate(&header, allRecord[i], 2);
				delete[]allRecord[i];
			}

			setNextid(ib.blockid());
			ib.setNextid(nextid);
			memcpy(buffer_, midbuf, BLOCK_SIZE);
			write();
			ib.write(x);

			int id = ib.blockid();
			q[0].iov_base = allRecord[allRecord.size() / 2]->iov_base;
			q[0].iov_len = allRecord[allRecord.size() / 2]->iov_len;
			q[1].iov_base = &id;
			q[1].iov_len = sizeof(int);
			height--;
			up = true;
		}
	}

	if (height < 0&&up)
	{
		unsigned char* headbuf = GetNewBlock();
		IndexBlock head;
		head.attach(headbuf);
		unsigned char header = Record::initHeader(false, false);
		head.allocate(&header, q, 2);
		head.setNextid(ib.blockid());
		root.setHead(head.blockid());
		x.write(GetOffset(head.blockid()), (char*)headbuf, BLOCK_SIZE);
	}
}

unsigned char* IndexBlock::GetNewBlock()
{
	printRecord();
	unsigned char* newbuffer = new unsigned char[Block::BLOCK_SIZE];
	buf.push_back(newbuffer);
	DataBlock ib;
	ib.attach(newbuffer);

	unsigned long long len=0;
	x.length(len);
	if (len == GetOffset(root.getGarbage()))
	{
		ib.clear(root.getGarbage(), 0);
		root.setGarbage(root.getGarbage() + 1);
	}
	else
	{
		ib.clear(ib.blockid(), 0);
		root.setGarbage(ib.getGarbage());
	}
	return newbuffer;
}

void IndexBlock::jump(int blockid)
{
	x.read(GetOffset(blockid), (char*)buffer_, BLOCK_SIZE);
}

void IndexBlock::write()
{
	if (table != nullptr)
	{
		x.write(GetOffset(blockid()), (char*)buffer_, BLOCK_SIZE);
	}
}

void IndexBlock::write(File&x)
{
		x.write(GetOffset(blockid()), (char*)buffer_, BLOCK_SIZE);	
}

iovec* IndexBlock::Inrequire(iovec* key)
{
	gschema.open();
	std::pair<Schema::TableSpace::iterator, bool> tb = gschema.lookup(table);
	//获取需要的key和type
	int k = tb.first->second.key;

	int id = GetBlockId(key);
	File y;
	std::string a(table);
	y.open((a+".db").c_str());
	unsigned char buf[BLOCK_SIZE];
	y.read(GetOffset(id), (char*)buf, BLOCK_SIZE);
	DataBlock db;
	db.attach(buf);
	Record rd;
	for (int i = 0; i < db.getSlotsNum(); i++)
	{
		rd.attach(buf + db.getSlot(i));
		iovec* q = rd.getKey(k);
		int num = rd.fields();
		if (iovecCmp(key,q) == 0)
		{
			iovec* q = new iovec[num];
			for (int i = 0; i < num; i++)
			{
				iovec* p = rd.getKey(i);
				q[i].iov_base = p->iov_base;
				q[i].iov_len = p->iov_len;
			}
			return q;
		}
	}
	return nullptr;
}

void IndexBlock::BlockSort()
{
	//获取需要的key和type
	int slotNum = getSlotsNum();
	std::vector<int> offset;
	std::vector<std::pair<iovec*, int>> s;
	for (int i = 0; i < slotNum; i++) {
		offset.push_back(getSlot(i));
	}
	iovec** p = new iovec * [slotNum];
	Record* q = new Record[slotNum];
	Integer it;

	for (int i = 0; i < slotNum; i++) {
		it.decode((char*)buffer_ + offset[i], 8);
		q[i].attach(buffer_ + offset[i], it.get());

		//如果这个记录没有被删除
		if (!q[i].is_delete())
		{
			p[i] = q[i].getKey(0);
			std::pair<iovec*, int> a;
			a.first = p[i];
			a.second = offset[i];
			s.push_back(a);
		}
	}

	std::sort(s.begin(), s.end(), cmp);

	std::string aaaa((char*)s[0].first->iov_base);
	std::string bbb((char*)s[s.size()-1].first->iov_base);

	for (int i = 0; i < s.size(); i++)
	{
		setSlot(i, s[i].second);
		delete s[i].first;
	}
	setChecksum();
	s.clear();
	delete[] p;
	delete[] q;
}

void IndexBlock::clearGarbage()
{
	int num = getSlotsNum();
	Integer it;
	int offset = BLOCK_DEFAULT_FREESPACE;

	std::vector<iovec*> allRecord;//block内未被删除的记录
	unsigned char* header = new unsigned char;
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
			iovec* p = new iovec[iovcnt];
			record.ref(p, record.fields(), header);
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

	unsigned char* buf = new unsigned char[Block::BLOCK_SIZE];
	unsigned char* buffer = buffer_;
	memcpy(buf, buffer_, Block::BLOCK_SIZE);
	attach(buf);
	clear(0,blockid());
	for (int i = 0; i < allRecord.size(); i++)
	{
		allocate(header, allRecord[i], iovcnt); //由于原block有序，所以删掉一些记录后，依然有序，不用排序   
		delete allRecord[i];
	}
	memcpy(buffer, buf, Block::BLOCK_SIZE);
	attach(buffer);
	delete header;
	delete[] buf;

}



void IndexBlock::printRecord()
{	
		for (int i = 0; i < getSlotsNum(); i++)
		{
			iovec p[2];
			unsigned char header = ' ';
			Record rd;
			rd.attach(buffer_ + getSlot(i));
			rd.ref(p, 2, &header);
			std::cout << (char*)p[0].iov_base << "------>" << *(int*)p[1].iov_base << std::endl;
			std::cout << std::endl;
		}	
}

void IndexBlock::DeleteRecord(iovec* key)
{
	if (buffer_ == nullptr) return;
	Root root;
	unsigned char rootbuffer[Root::ROOT_SIZE];
	root.attach(rootbuffer);
	x.read(0, (char*)rootbuffer, Root::ROOT_SIZE);
	x.read(GetOffset(root.getHead()), (char*)buffer_, Block::BLOCK_SIZE);

	Record rd;
	while (!isLeaf())
	{
		int pos = FindPlace(key);
		rd.attach(buffer_ + getSlot(pos));
		iovec* q = rd.getKey(1);
		x.read(
			GetOffset(*(int*)q->iov_base),
			(char*)buffer_,
			Block::BLOCK_SIZE);
	}

	//找到对应的记录后删掉这条记录，并看情况是否向上调整
	int pos = FindPlace(key);
	rd.attach(buffer_ + getSlot(pos));
	rd.setheader(Record::initHeader(false, true));
	clearGarbage();
	

	//如果删除后使用的空间小于一半或者INDEX只有一层
	if (getFreeLength() < BLOCK_SIZE / 2||path.size()==0)
	{
		write();
		return;
	}
	else
	{
		up(key);
	}

}

void IndexBlock::up(iovec*key)
{
	unsigned char fabuf[BLOCK_SIZE];
	unsigned char brobuf[BLOCK_SIZE];

	IndexBlock fa;
	IndexBlock bro;

	fa.attach(fabuf);
	bro.attach(brobuf);

	int height = path.size()-1;
	bool up = true;

	while (height >= 0&&up)
	{
		x.read(GetOffset(path[height]), (char*)fabuf, BLOCK_SIZE);

		bool right = true;
		up = false;
		if (fa.getNextid() == blockid())
		{
			Record rd;
			rd.attach(fabuf + fa.getSlot(0));
			iovec* p = rd.getKey(1);
			x.read(GetOffset(*(int*)p[1].iov_base), (char*)brobuf, BLOCK_SIZE);
			delete[]p;
			right = false;
		}
		else
		{
			int pos = fa.FindPlace(key);
			x.read(GetOffset(pos), (char*)brobuf, BLOCK_SIZE);
		}

		if (bro.getFreeLength() < BLOCK_SIZE / 2)
		{
			BorrowRecord(brobuf, fabuf, right);
		}
		else
		{
			merge(brobuf, fabuf);
			up = true;
		}
		height--;
		if (height < 0)
			break;
		else
			x.read(GetOffset(path[height]), (char*)fabuf, BLOCK_SIZE);
	}
}


void IndexBlock::BorrowRecord(unsigned char* brobuf, unsigned char* fabuf,bool right)
{
	//往左借的话不影响，往右借需要修改父结点里的key


	IndexBlock fa;
	IndexBlock bro;

	fa.attach(fabuf);
	bro.attach(brobuf);

	int pos;
	if (right)
		pos = bro.getSlotsNum() - 1;
	else
		pos = 0;

	Record rd;
	rd.attach(brobuf + bro.getSlot(pos));

	iovec p[2];
	unsigned char header = ' ';
	rd.ref(p,2,&header);
	allocate(&header,p,2);
	rd.setheader(Record::initHeader(true,true));
	bro.clearGarbage();
	bro.write(x);
	BlockSort();
	write();

	if (right)
	{		
		for (int i = 0; i < fa.getSlotsNum(); i++)
		{
			rd.attach(fabuf + fa.getSlot(i));
			iovec* ptr = rd.getKey(1);
			int x = *(int*)ptr->iov_base;
			if (x == blockid())
			{
				rd.setheader(Record::initHeader(isLeaf(), true));
				fa.clearGarbage();
				iovec q[2]; 
				Record d;
				d.attach(buffer_ + getSlot(0));
				iovec* key = rd.getKey(0);
				q[0].iov_base = key->iov_base;
				q[0].iov_len = key->iov_len;
				q[1].iov_base = &x;
				q[1].iov_len = sizeof(int);
				unsigned char header = Record::initHeader(false,false);
				fa.allocate(&header,q,2);
				fa.write(this->x);
				delete[]key;
			}
			delete[]ptr;
		}
	}
}

void IndexBlock::merge(unsigned char* brobuf, unsigned char* fabuf)
{
	IndexBlock fa;
	IndexBlock bro;

	fa.attach(fabuf);
	bro.attach(brobuf);

	//如果是最左边的结点合并
	//把父结点指向bro的key拿下来，然后把那条记录删掉
	//都合并到现在的block内
	if (blockid() == fa.getNextid())
	{
		int id = bro.getNextid();
		Record rd;
		rd.attach(fabuf + fa.getSlot(0));
		iovec* p = rd.getKey(0);
		iovec q[2];
		q[0].iov_base = p->iov_base;
		q[0].iov_len = p->iov_len;
		q[1].iov_base=& id;
		q[1].iov_len = sizeof(int);

		std::vector<iovec*>allRecord;
		bro.getAllRecord(allRecord);
		unsigned char header = Record::initHeader(isLeaf(),false);
		for (int i = 0; i < allRecord.size(); i++)
		{
			allocate(&header,allRecord[i],2);
		}
		if(!isLeaf())
			allocate(&header, q, 2);

		rd.setheader(Record::initHeader(isLeaf(), true));
		fa.clearGarbage();

		BlockSort();
		fa.BlockSort();

		write();
		fa.write(x);
		
		bro.setGarbage(root.getGarbage());
		root.setGarbage(bro.blockid());
		bro.write();
		delete[]p;
	}

	else
	{
		//如果不是的话，那么就把now的记录都合并到bro内

		int id = getNextid();
		Record rd;
		iovec* p;

		for (int i = 0; i < fa.getSlotsNum(); i++)
		{
			rd.attach(fabuf + fa.getSlot(i));
			iovec* ptr = rd.getKey(1);
			int x = *(int*)ptr->iov_base;
			if (x == blockid())
			{
				p = rd.getKey(0);
			}
			rd.setheader(Record::initHeader(isLeaf(), true));
			delete[]ptr;
		}

		iovec q[2];
		q[0].iov_base = p->iov_base;
		q[0].iov_len = p->iov_len;
		q[1].iov_base = &id;
		q[1].iov_len = sizeof(int);

		std::vector<iovec*>allRecord;
		getAllRecord(allRecord);

		unsigned char header = Record::initHeader(isLeaf(), false);
		for (int i = 0; i < allRecord.size(); i++)
		{
			bro.allocate(&header, allRecord[i], 2);
		}

		if(!isLeaf())
			bro.allocate(&header, q, 2);

		fa.clearGarbage();

		BlockSort();
		fa.BlockSort();

		write();
		fa.write(x);

		bro.setGarbage(root.getGarbage());
		root.setGarbage(bro.blockid());
		bro.write();
	}
}

}; // namespace db