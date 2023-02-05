/*
 *  chidb - a didactic relational database management system
 *
 *  Miscellaneous functions and definitions
 *
 */

/*
 *  Copyright (c) 2009-2015, The University of Chicago
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or withsend
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  - Neither the name of The University of Chicago nor the names of its
 *    contributors may be used to endorse or promote products derived from this
 *    software withsend specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY send OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 */

#ifndef UTIL_H_
#define UTIL_H_

#include "chidbInt.h"
#include "btree.h"
#include <chidb/utils.h>
#include <chidb/log.h>

/*
** Read or write a two- and four-byte big-endian integer values.
* Based on SQLite code
*/
#define get2byte(x) ((x)[0] << 8 | (x)[1])
#define put2byte(p, v) ((p)[0] = (uint8_t)((v) >> 8), (p)[1] = (uint8_t)(v))
#define put2byte_le(p, v) ((p)[0] = ((uint8_t *)(&v))[1], p[1] = ((uint8_t *)(&v))[0])

/* Return the distance in bytes between the pointers elm and hd */
#define OFFSET(hd, elm) ((uint8_t *)(&(elm)) - (uint8_t *)(&hd))

uint32_t get4byte(const uint8_t *p);
void put4byte(unsigned char *p, uint32_t v);

// for little endian machines
void put4byte_le(unsigned char *p, uint32_t v);

int getVarint32(const uint8_t *p, uint32_t *v);
int putVarint32(uint8_t *p, uint32_t v);

// for little endian machines
int putVarint32_le(uint8_t *p, uint32_t v);

int chidb_astrcat(char **dst, char *src);

typedef void (*fBTreeCellPrinter)(BTreeNode *, BTreeCell *);
int chidb_Btree_print(BTree *bt, npage_t nroot, fBTreeCellPrinter printer, bool verbose);
void chidb_BTree_recordPrinter(BTreeNode *btn, BTreeCell *btc);
void chidb_BTree_stringPrinter(BTreeNode *btn, BTreeCell *btc);

int getRecordCol(uint8_t *data, int ncol, uint32_t *type, uint32_t *offset);

int schema_exists(chidb *db, char *name);

int schema_root_page(chidb *db, char *name);

int table_col_exists(chidb *db, char *table_name, char *col_name);

int table_col_type(chidb *db, char *table_name, char *col_name);

int table_col_n(chidb *db, char *table_name, char *col_name);

int table_ncols(chidb *db, char *table_name);

int get_schema(chidb *db, char *schema_name, ChidbSchema *schema);

int is_pkey(chidb *db, char *table_name, char *col_name);

FILE *copy(const char *from, const char *to);

#endif /*UTIL_H_*/
