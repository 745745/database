////
// @file tableTest.cc
// @brief
// 测试存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/table.h>
#include<db/block.h>
#include<iostream>
#include<db/record.h>
using namespace db;

extern int db::GetOffset(int blockid);

char *randstr(char *str, const int len,int ran)
{
    srand(ran);
    int i;
    for (i = 0; i < len-1; ++i) {
        switch ((rand() % 3)) {
        case 1:
            str[i] = 'A' + rand() % 26;
            break;
        case 2:
            str[i] = 'a' + rand() % 26;
            break;
        default:
            str[i] = '0' + rand() % 10;
            break;
        }
    }
    str[i] = '\0';
    return str;
}



TEST_CASE("db/table.h")
{
    SECTION("insert && inrequire")
    {
         Schema schema;
         int ret = schema.open();
         REQUIRE(ret == S_OK);

         DataType *dt = findDataType("VARCHAR");

        // 填充关系
         RelationInfo relation;
         relation.path = "table.dat";

        // id char(20) varchar
         FieldInfo field;
         field.name = "id";
         field.index = 0;
         field.length = 8;
         field.type = "VARCHAR";
         relation.fields.push_back(field);

         field.name = "phone";
         field.index = 1;
         field.length = 20;
         relation.fields.push_back(field);

         field.name = "name";
         field.index = 2;
         field.length = -255;
         relation.fields.push_back(field);

         relation.count = 3;
         relation.key = 0;

         ret = schema.create("table1", relation);

         const char *name = "table1";
         File x;
         Table tb;
         struct iovec iov[4];
         struct iovec iov1[4];
         struct iovec iov2[4];
         char table[10];//"table.db"
         char table1[10]="qqqqqzzzz";
         char table2[10];

         int type = 1;
         const char *hello = "hello, world";
		 const char* hello1 = "the world";
         size_t length = strlen(hello) + 1;

         iov[0].iov_base = (void *) table;
         iov[0].iov_len = 10;
         iov[1].iov_base = &type;
         iov[1].iov_len = sizeof(int);
         iov[2].iov_base = (void *) hello;
         iov[2].iov_len = strlen(hello) + 1;
         iov[3].iov_base = &length;
         iov[3].iov_len = sizeof(size_t);

         iov1[0].iov_base = (void *)table1;
         iov1[0].iov_len = 10;
         iov1[1].iov_base = &type;
         iov1[1].iov_len = sizeof(int);
         iov1[2].iov_base = (void *) hello1;
         iov1[2].iov_len = strlen(hello) + 1;
         iov1[3].iov_base = &length;
         iov1[3].iov_len = sizeof(size_t);

         iov2[0].iov_base = (void *) table2;
         iov2[0].iov_len = 10;
         iov2[1].iov_base = &type;
         iov2[1].iov_len = sizeof(int);
         iov2[2].iov_base = (void *) hello;
         iov2[2].iov_len = strlen(hello) + 1;
         iov2[3].iov_base = &length;
         iov2[3].iov_len = sizeof(size_t);

         std::vector<std::string> key;

         for (int i = 0; i <=5000; i++)
        {
            std::cout << i<<std::endl;
            randstr(table, 10,i*i*i+i);
            tb.insert(iov, 4, name);
            key.push_back((char*)iov->iov_base);
        }

		 tb.insert(iov1, 4, name);

		 for (int i = 5001; i <= 10000; i++)
		 {
			 std::cout << i << std::endl;
			 randstr(table, 10, i * i * i + i);
			 tb.insert(iov, 4, name);
			 key.push_back((char*)iov->iov_base);
		 }

		 IndexBlock ib(name);
		 iovec* q = tb.inquire(&iov1[0], (char*)name);
		 REQUIRE(memcmp(q[2].iov_base,hello1,strlen(hello1)+1)==0);

         x.open("table1.db");
         unsigned char *buf =new unsigned char [Block::BLOCK_SIZE];
         x.read(0, (char *) buf, Root::ROOT_SIZE);
         Root rt;
         rt.attach(buf);
         REQUIRE(rt.getType() == BLOCK_TYPE_DATA);
         int magic_number;
         memcpy(
            &magic_number,
            buf + Root::ROOT_MAGIC_OFFSET,
            Root::ROOT_MAGIC_SIZE);
         REQUIRE(magic_number==Root::ROOT_MAGIC_NUMBER);

         DataBlock db;
         x.read(Root::ROOT_SIZE, (char *) buf, Block::BLOCK_SIZE);
         db.attach(buf);
         db.clearGarbage(x);
         REQUIRE(db.getType() == BLOCK_TYPE_DATA);
         memcpy(
            &magic_number,
            buf + Block::BLOCK_MAGIC_OFFSET,
            Block::BLOCK_MAGIC_SIZE);
         REQUIRE(magic_number == Block::BLOCK_MAGIC_NUMBER);
		 REQUIRE(db.getSlotsNum() != 0);
         x.close();
    }

    SECTION("remove")
    {
  //       Schema schema;
  //       int ret = schema.open();
  //       REQUIRE(ret == S_OK);

  //       DataType *dt = findDataType("VARCHAR");

  //      // 填充关系
  //       RelationInfo relation;
  //       relation.path = "table.dat";

  //      // id char(20) varchar
  //       FieldInfo field;
  //       field.name = "id";
  //       field.index = 0;
  //       field.length = 8;
  //       field.type = "VARCHAR";
  //       relation.fields.push_back(field);

  //       field.name = "phone";
  //       field.index = 1;
  //       field.length = 20;
  //       relation.fields.push_back(field);

  //       field.name = "name";
  //       field.index = 2;
  //       field.length = -255;
  //       relation.fields.push_back(field);

  //       relation.count = 3;
  //       relation.key = 0;

  //       ret = schema.create("deletetable", relation);

  //       const char *name = "deletetable";
  //       File x;
  //       Table tb;
  //       struct iovec iov[4];
  //       struct iovec iov1[4];
  //       struct iovec iov2[4];
  //       char table[10];
  //       char table1[10];
  //       char table2[10];

  //       int type = 1;
  //       const char *hello = "hello, world";
  //       size_t length = strlen(hello) + 1;

  //       iov[0].iov_base = (void *) table;
  //       iov[0].iov_len = 10;
  //       iov[1].iov_base = &type;
  //       iov[1].iov_len = sizeof(int);
  //       iov[2].iov_base = (void *) hello;
  //       iov[2].iov_len = strlen(hello) + 1;
  //       iov[3].iov_base = &length;
  //       iov[3].iov_len = sizeof(size_t);

  //       iov1[0].iov_base = (void *) table1;
  //       iov1[0].iov_len = 10;
  //       iov1[1].iov_base = &type;
  //       iov1[1].iov_len = sizeof(int);
  //       iov1[2].iov_base = (void *) hello;
  //       iov1[2].iov_len = strlen(hello) + 1;
  //       iov1[3].iov_base = &length;
  //       iov1[3].iov_len = sizeof(size_t);

  //       iov2[0].iov_base = (void *) table2;
  //       iov2[0].iov_len = 10;
  //       iov2[1].iov_base = &type;
  //       iov2[1].iov_len = sizeof(int);
  //       iov2[2].iov_base = (void *) hello;
  //       iov2[2].iov_len = strlen(hello) + 1;
  //       iov2[3].iov_base = &length;
  //       iov2[3].iov_len = sizeof(size_t);

  //       std::vector<std::string> key;

  //       for (int i = 0; i <= 20000; i++) 
		// {
  //          std::cout << i << std::endl;
  //          randstr(table, 10, i * i * i + i);
  //          tb.insert(iov, 4, name);
  //          key.push_back((char *) iov->iov_base);
		//}

  //       for (int i = 0; i <= 20000; i++)
  //      {
  //          std::cout << "delete:  " << i << std::endl;
  //          iovec p;
  //          p.iov_base = (void *) key[i].c_str();
  //          p.iov_len = 10;
  //          tb.remove(&p, name);
  //      }

		// x.open("deletetable.db");
  //       unsigned char *buf = new unsigned char[Block::BLOCK_SIZE];
  //       x.read(0, (char *) buf, Root::ROOT_SIZE);
  //       Root rt;
  //       rt.attach(buf);
  //       REQUIRE(rt.getType() == BLOCK_TYPE_DATA);
  //       int magic_number;
  //       memcpy(
  //          &magic_number,
  //          buf + Root::ROOT_MAGIC_OFFSET,
  //          Root::ROOT_MAGIC_SIZE);
  //       REQUIRE(magic_number == Root::ROOT_MAGIC_NUMBER);

  //       DataBlock db;
  //       x.read(Root::ROOT_SIZE, (char *) buf, Block::BLOCK_SIZE);
  //       db.attach(buf);
  //       db.clearGarbage(x);
  //       REQUIRE(db.getType() == BLOCK_TYPE_DATA);
  //       memcpy(
  //          &magic_number,
  //          buf + Block::BLOCK_MAGIC_OFFSET,
  //          Block::BLOCK_MAGIC_SIZE);
  //       REQUIRE(magic_number == Block::BLOCK_MAGIC_NUMBER);
  //       REQUIRE(db.getSlotsNum() == 1);
  //       x.close();
    }

    SECTION("update")
    {
        //Schema schema;
        //int ret = schema.open();
        //REQUIRE(ret == S_OK);

        //DataType *dt = findDataType("VARCHAR");

        //// 填充关系
        //RelationInfo relation;
        //relation.path = "table.dat";

        //// id char(20) varchar
        //FieldInfo field;
        //field.name = "id";
        //field.index = 0;
        //field.length = 8;
        //field.type = "VARCHAR";
        //relation.fields.push_back(field);

        //field.name = "phone";
        //field.index = 1;
        //field.length = 20;
        //relation.fields.push_back(field);

        //field.name = "name";
        //field.index = 2;
        //field.length = -255;
        //relation.fields.push_back(field);

        //relation.count = 3;
        //relation.key = 0;

        //ret = schema.create("updatetable", relation);

        //const char *name = "updatetable";
        //File x;
        //Table tb;
        //struct iovec iov[4];
        //struct iovec iov1[4];
        //struct iovec iov2[4];
        //char table[10];
        //char table1[10];
        //char table2[10];

        //int type = 1;
        //const char *hello = "hello, world";
        //size_t length = strlen(hello) + 1;

        //iov[0].iov_base = (void *) table;
        //iov[0].iov_len = 10;
        //iov[1].iov_base = &type;
        //iov[1].iov_len = sizeof(int);
        //iov[2].iov_base = (void *) hello;
        //iov[2].iov_len = strlen(hello) + 1;
        //iov[3].iov_base = &length;
        //iov[3].iov_len = sizeof(size_t);

        //iov1[0].iov_base = (void *) table1;
        //iov1[0].iov_len = 10;
        //iov1[1].iov_base = &type;
        //iov1[1].iov_len = sizeof(int);
        //iov1[2].iov_base = (void *) hello;
        //iov1[2].iov_len = strlen(hello) + 1;
        //iov1[3].iov_base = &length;
        //iov1[3].iov_len = sizeof(size_t);

        //iov2[0].iov_base = (void *) table2;
        //iov2[0].iov_len = 10;
        //iov2[1].iov_base = &type;
        //iov2[1].iov_len = sizeof(int);
        //iov2[2].iov_base = (void *) hello;
        //iov2[2].iov_len = strlen(hello) + 1;
        //iov2[3].iov_base = &length;
        //iov2[3].iov_len = sizeof(size_t);

        //std::vector<std::string> key;
        //for (int i = 0; i <= 1000; i++) {
        //    std::cout << i << std::endl;
        //    randstr(table, 10, i * i * i + i);
        //    tb.insert(iov, 4, name);
        //    key.push_back((char *) iov->iov_base);
        //}

        //for (int i = 0; i <= 1000; i++) {
        //    std::cout << "update:  " << i << std::endl;
        //    iovec p;
        //    p.iov_base = (void *) key[i].c_str();
        //    p.iov_len = 10;

        //    iovec q;
        //    q.iov_base = "the world";
        //    q.iov_len = strlen((char *) q.iov_base) + 1;
        //    tb.update(&p, &q, 2, name);
        //}

        //x.open("updatetable.db");
        //unsigned char *buf = new unsigned char[Block::BLOCK_SIZE];
        //x.read(0, (char *) buf, Root::ROOT_SIZE);
        //Root rt;
        //rt.attach(buf);
        //REQUIRE(rt.getType() == BLOCK_TYPE_DATA);
        //int magic_number;
        //memcpy(
        //    &magic_number,
        //    buf + Root::ROOT_MAGIC_OFFSET,
        //    Root::ROOT_MAGIC_SIZE);
        //REQUIRE(magic_number == Root::ROOT_MAGIC_NUMBER);
        //
        //x.read(GetOffset(rt.getHead()), (char *) buf, Block::BLOCK_SIZE);
        //DataBlock db;
        //db.attach(buf);
        //REQUIRE(db.getType() == BLOCK_TYPE_DATA);
        //memcpy(
        //    &magic_number,
        //    buf + Block::BLOCK_MAGIC_OFFSET,
        //    Block::BLOCK_MAGIC_SIZE);
        //REQUIRE(magic_number == Block::BLOCK_MAGIC_NUMBER);

        //Record rd;
        //Integer it;
        //it.decode((char *) buf + db.getSlot(0), 8);
        //rd.attach(buf + db.getSlot(0), it.get());
        //REQUIRE(memcmp((char *) rd.getKey(2), "the world", strlen("the world") + 1));
        //x.close();
    }
}