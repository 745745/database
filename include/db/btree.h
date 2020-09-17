#ifndef BTREE
#define BTREE
#include <db/table.h>
#include<db/schema.h>
#include<db/block.h>
#include<db/record.h>
#include<db/integer.h>
#include<iostream>

extern int initFile();
namespace db
{
	extern int GetOffset(int blockid);


	class IndexBlock : public Block
	{
	private:
		const char* table;
		unsigned char* rootbuffer;
		File x;
		std::vector<int> path;
		std::vector<unsigned char*>buf;
		Root root;//root一直存在内存里，最后析构的时候写入磁盘

	public:
		IndexBlock(const char* table);
		IndexBlock();
		unsigned long long initFile();


		//插入索引
		void InsertRecord(iovec* key, int blockid);

		//写入文件
		void write();

		//给定key返回其datablock的id
		int GetBlockId(iovec* key);

		//判断是否是叶子，true为是叶子
		bool isLeaf();


		//找key在block的位置，返回的是刚好比其大的key的位置
		int FindPlace(iovec* key);

		//分裂并向上调整
		void Spilt(iovec* key);

		//分配一个新的block
		unsigned char* GetNewBlock();

		void jump(int blockid);

		void write(File& x);

		//查询
		iovec* Inrequire(iovec* key);

		void clearGarbage();

		void BlockSort();

		//删除某条记录
		void DeleteRecord(iovec* key);

		//向上调整
		void up(iovec* key);

		//借记录 right代表是否往右借
		void BorrowRecord(unsigned char*brobuf, unsigned char* fabuf, bool right);

		//合并
		void merge(unsigned char* brobuf, unsigned char* fabuf);


		~IndexBlock()
		{
			for (int i = 0; i < buf.size(); i++)
				delete[]buf[i];
			x.write(0, (char*)rootbuffer, Root::ROOT_SIZE);
			if(buffer_!=nullptr)
				delete[]buffer_;
			if(rootbuffer!=nullptr)
				delete[]rootbuffer;
		}

		void printRecord();
	};

	class block_iterator
	{
	public:
		char* table;
		unsigned char* buffer;
		unsigned char* rootbuffer;
		std::vector<unsigned char*> next; //所有新分配的内存都进next,析构时统一释放
		int blockid;
		File x;
		DataBlock db;
		Root root;


		block_iterator(const char* table)
		{
			gschema.open();
			std::pair<Schema::TableSpace::iterator, bool> tb = gschema.lookup(table);

			this->table = (char*)table;

			buffer = new unsigned char[Block::BLOCK_SIZE];
			db.attach(buffer);
			std::string a(table);
			x.open((a + ".db").c_str());
			initFile();
			rootbuffer = new unsigned char[Root::ROOT_SIZE];
			root.attach(rootbuffer);
			x.read(0, (char*)rootbuffer, Root::ROOT_SIZE);			

			blockid = root.getHead();			
			int ddd = root.getGarbage();
			x.read(GetOffset(blockid), (char*)buffer, Block::BLOCK_SIZE);
		}

		void write();
		unsigned char* GetNewBlock();
		void BlockSort();
		void Spilt(iovec* record, int iovcnt, const char* table);
		void jump(int blockid);
		void operator++();
		bool insert(iovec* iov, int iovcnt);
		bool canInsert(iovec* iov, int iovcnt);
		bool remove(iovec* key);
		int nextid();





		~block_iterator()
		{
			x.write(0, (char*)rootbuffer, Root::ROOT_SIZE);			
			delete[] buffer;
			delete[] rootbuffer;
			for (int i = 0; i < next.size(); i++)
			{
				delete[] next[i];
			}
		}
	};
} // namespace db

#endif BTREE