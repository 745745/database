////
// @file record.h
// @brief
// 定义数据库记录
// 采用类似MySQL的记录方案，一条记录分为四个部分：
// 记录总长度+字段长度数组+Header+字段
// 1. 记录总长度从最开始算；
// 2. 字段长度是一个逆序变长数组，记录每条记录从Header开始的头部偏移量位置；
// 3. Header存放一些相关信息，1B；
// 4. 然后是各字段顺序摆放；
//
// | T | M | x | x | x | x | x | x |
//   ^   ^
//   |   |
//   |   +-- 最小记录
//   +-- tombstone
//
// 记录的分配按照4B对齐，同时要求block头部至少按照4B对齐
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_RECORD_H__
#define __DB_RECORD_H__

#include <utility>
#include "./config.h"
#include "./integer.h"

namespace db {

// 物理记录
// TODO: 长度超越一个block？
class Record
{
  public:
    static const int HEADER_SIZE = 1; // 头部1B
    static const int ALIGN_SIZE = 8;  // 按8B对齐


    //头部1B 8位，前四位来记录是否跨block，后四位来记录是否被删除
    static const int MASK_OVER_BLOCK_TRUE = 0xF0;
    static const int MASK_OVER_BLOCK_FALSE = 0x00;

    static const int MASK_DELETE_TRUE=0x0F;
    static const int MASK_DELETE_FALSE=0x0;

    //之前没有想到，刚好可以用这个over_block来标识index block内的节点是否指向datablock
    //over_block=1,说明指向datablock
    //如果指向datablock,OVER_BLOCK=1 如果被删除,IS_DELETE=1
    static unsigned char initHeader(bool OVER_BLOCK, bool IS_DELETE) 
    { 
        char header = 0x00;
        if (OVER_BLOCK)
            header = header ^ MASK_OVER_BLOCK_TRUE;
        else
            header = header ^ MASK_OVER_BLOCK_FALSE;
        if (IS_DELETE)
            header = header ^ MASK_DELETE_TRUE;
        else
            header = header ^ MASK_DELETE_FALSE;
        return header;
    }

    //记录是否跨记录,如果指向datablock为true
    bool IsOverBlock();

    //是否被删除,true为已删除
    bool is_delete();

  private:
    unsigned char *buffer_; // 记录buffer
    unsigned short length_; // buffer长度

  public:
    Record()
        : buffer_(NULL)
        , length_(0)
    {}

    // 关联buffer
    inline void attach(unsigned char *buffer, unsigned short length)
    {
        buffer_ = buffer;
        length_ = length;
    }

	inline void attach(unsigned char* buffer)
	{
		buffer_ = buffer;
		Integer it;
		it.decode((char*)buffer_,8);
		length_=it.get();
	}


    // 整个记录长度+header偏移量
    static std::pair<size_t, size_t> size(const iovec *iov, int iovcnt);

    // 向buffer里写各个域，返回按照对齐后的长度
    size_t set(const iovec *iov, int iovcnt, const unsigned char *header);
    // 从buffer获取各字段(深拷贝)
    bool get(iovec *iov, int iovcnt, unsigned char *header);
    // 从buffer引用各字段(浅拷贝)
    bool ref(iovec *iov, int iovcnt, unsigned char *header);
    // TODO:
    void dump(char *buf, size_t len);

    // 获得记录总长度，包含头部+变长偏移数组+长度+记录
    size_t length();
    // 获取记录字段个数
    size_t fields();

    //key从0开始
    iovec *getKey(int key);

    void setheader(unsigned char header);
};

} // namespace db

#endif // __DB_RECORD_H__
