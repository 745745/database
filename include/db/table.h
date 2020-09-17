////
// @file table.h
// @brief
// 存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_TABLE_H__
#define __DB_TABLE_H__

#include <cmath>
#include<algorithm>
#include "block.h"
#include"btree.h"
#include "./schema.h"
#include "integer.h"




namespace db 
{
	extern int iovecCmp(iovec* a, iovec* b);
	extern int GetOffset(int);


	bool cmp(std::pair<iovec*, int> a, std::pair<iovec*, int> b);

	bool cmp1(iovec* a, iovec* b);

	////
	// @brief
	// 表操作接口
	//

	class Table
	{
	public:
		// 创建表
		int create(const char* name, RelationInfo& info);
		
		// 打开表
		int open(const char* name);

		//插入
		int insert(struct iovec* record, size_t iovcnt, const char* table);

		//移除
		int remove(iovec* keyinf, const char* table);

		//更新 key用来确定record，info为要修改的信息，fieldkey为要修改的字段id
		int update(iovec* keyinfo, iovec* info, int fieldkey, const char* table);

		//用key查找记录
		iovec* inquire(iovec* key, char* table);
	};



} // namespace db

#endif // __DB_TABLE_H__