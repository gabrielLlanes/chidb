/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors -- header
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

#ifndef DBM_CURSOR_H_
#define DBM_CURSOR_H_

#include "chidbInt.h"
#include "btree.h"
#include <chidb/log.h>

#define CHIDB_CURSOR_EMPTY_BTREE 1
#define CHIDB_CURSOR_LAST_ENTRY 2
#define CHIDB_CURSOR_FIRST_ENTRY 3
#define CHIDB_CURSOR_KEY_NOT_FOUND 4

// #define CHIDB_CURSOR_NODE_ENTRY

typedef enum chidb_dbm_cursor_type
{
    CURSOR_UNSPECIFIED,
    CURSOR_READ,
    CURSOR_WRITE
} chidb_dbm_cursor_type_t;

typedef enum chidb_dbm_cursor_tree_type
{
    TABLE_CURSOR,
    INDEX_CURSOR
} chidb_dbm_cursor_tree_type;

// typedef enum chidb_dbm_cursor_node_entry_type
// {
//     LEFTMOST,
//     RIGHT_PAGE,
//     MIDDLE
// } cursor_node_entry_type;

typedef struct chidbm_dbm_cursor_node_entry
{
    BTreeNode *node;
    // cursor_node_entry_type type;

    // valid only if is_right_page is 0. if the node is an internal type, then this is
    // the key whose associated child page is on the path to the current key of the cursor.
    // if the node is a leaf type, then it is simply the same as the current key of the cursor.
    chidb_key_t key;

    ncell_t ncell;
} cursor_node_entry;

typedef struct chidb_dbm_cursor
{
    chidb_dbm_cursor_type_t type; //
    chidb_dbm_cursor_tree_type tree_type;
    BTree *bt;                       //
    npage_t root_page_n;             //
    cursor_node_entry *node_entries; //
    uint32_t col_n;
    uint32_t curr_key;
    uint32_t nNodes; // equal to number of nodes in the array of nodes.
    /* Your code goes here */

} chidb_dbm_cursor_t;

/* Cursor function definitions go here */

int chidb_Cursor_open(chidb_dbm_cursor_t **cursor, chidb_dbm_cursor_type_t type, BTree *bt, npage_t npage, uint32_t col_n);

int chidb_Cursor_freeCursor(chidb_dbm_cursor_t *cursor);

int realloc_nodes(chidb_dbm_cursor_t *cursor, uint32_t size);

int chidb_Cursor_setPathNode(chidb_dbm_cursor_t *cursor, npage_t page_n, ncell_t ncell, uint32_t i);

int chidb_Cursor_rewind(chidb_dbm_cursor_t *cursor);

// int chidb_Cursor_rewindTable(chidb_dbm_cursor_t *cursor);

// int chidb_Cursor_rewindIndex(chidb_dbm_cursor_t *cursor);

// int chidb_Cursor_next(chidb_dbm_cursor_t *cursor);
int chidb_Cursor_get(chidb_dbm_cursor_t *cursor, BTreeCell **cell);

int chidb_Cursor_next(chidb_dbm_cursor_t *cursor);

// int chidb_Cursor_indexNext(chidb_dbm_cursor_t *cursor);

int chidb_Cursor_prev(chidb_dbm_cursor_t *cursor);

int chidb_Cursor_setKey(chidb_dbm_cursor_t *cursor, chidb_key_t key, int index);

int chidb_Cursor_seek(chidb_dbm_cursor_t *cursor, chidb_key_t key);

int chidb_Cursor_seekGt(chidb_dbm_cursor_t *cursor, chidb_key_t key);

int chidb_Cursor_seekGte(chidb_dbm_cursor_t *cursor, chidb_key_t key);

int chidb_Cursor_seekLt(chidb_dbm_cursor_t *cursor, chidb_key_t key);

int chidb_Cursor_seekLte(chidb_dbm_cursor_t *cursor, chidb_key_t key);

#endif /* DBM_CURSOR_H_ */