﻿////
// @file datatype.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <algorithm>
#include <db/datatype.h>

namespace db 
{

bool compareChar(const void *x, const void *y, size_t sx, size_t sy)
{
    //返回值为true说明x<y
    return strncmp(
               (const char *) x, (const char *) y, std::max<size_t>(sx, sy)) <
           0;
}

bool copyChar(void *x, const void *y, size_t sx, size_t sy)
{
    if (sx < sy) return false;
    ::memcpy(x, y, sy);
    return true;
}

bool compareInt(const void *x, const void *y, size_t sx, size_t sy)
{
    return (ptrdiff_t) x < (ptrdiff_t) y;
}

bool copyInt(void *x, const void *y, size_t sx, size_t sy)
{
    ::memcpy(x, y, sy);
    return true;
}

DataType *findDataType(const char *name)
{
    static DataType gdatatype[] = {
        {"CHAR", 65535, compareChar, copyChar},     // 0
        {"VARCHAR", -65535, compareChar, copyChar}, // 1
        {"TINYINT", 1, compareInt, copyInt},        // 2
        {"SMALLINT", 2, compareInt, copyInt},       // 3
        {"INT", 4, compareInt, copyInt},            // 4
        {"BIGINT", 8, compareInt, copyInt},         // 5
        {},                                         // x
    };

    int index = 0;
    do {
        if (gdatatype[index].name == NULL)
            break;
        else if (strcmp(gdatatype[index].name, name) == 0)
            return &gdatatype[index];
        else
            ++index;
    } while (true);
    return NULL;
}

} // namespace db