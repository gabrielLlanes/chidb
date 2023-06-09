/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine operations.
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

#include "dbm.h"
#include "btree.h"
#include "record.h"
#include "util.h"

/* Function pointer for dispatch table */
typedef int (*handler_function)(chidb_stmt *stmt, chidb_dbm_op_t *op);

/* Single entry in the instruction dispatch table */
struct handler_entry
{
    opcode_t opcode;
    handler_function func;
};

/* This generates all the instruction handler prototypes. It expands to:
 *
 * int chidb_dbm_op_OpenRead(chidb_stmt *stmt, chidb_dbm_op_t *op);
 * int chidb_dbm_op_OpenWrite(chidb_stmt *stmt, chidb_dbm_op_t *op);
 * ...
 * int chidb_dbm_op_Halt(chidb_stmt *stmt, chidb_dbm_op_t *op);
 */
#define HANDLER_PROTOTYPE(OP) int chidb_dbm_op_##OP(chidb_stmt *stmt, chidb_dbm_op_t *op);
FOREACH_OP(HANDLER_PROTOTYPE)

/* Ladies and gentlemen, the dispatch table. */
#define HANDLER_ENTRY(OP) {Op_##OP, chidb_dbm_op_##OP},

int realloc_reg(chidb_stmt *stmt, uint32_t size);

int realloc_cur(chidb_stmt *stmt, uint32_t size);

struct handler_entry dbm_handlers[] =
    {
        FOREACH_OP(HANDLER_ENTRY)};

int chidb_dbm_op_handle(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return dbm_handlers[op->opcode].func(stmt, op);
}

/*** INSTRUCTION HANDLER IMPLEMENTATIONS ***/

int chidb_dbm_op_Noop(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return CHIDB_OK;
}

int chidb_dbm_op_OpenRead(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chilog(DEBUG, "OPEN READ root page %d, cursor number %d, with %d cols",
           stmt->reg[op->p2].value.i, op->p1, op->p3);
    if (stmt->nCursors <= op->p1)
    {
        realloc_cur(stmt, op->p1 + 1);
    }
    chidb_dbm_cursor_t *cursor;
    chidb_Cursor_open(&cursor, CURSOR_READ, stmt->db->bt, stmt->reg[op->p2].value.i, op->p3);
    stmt->cursors[op->p1] = *cursor;
    return CHIDB_OK;
}

int chidb_dbm_op_OpenWrite(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if (stmt->nCursors <= op->p1)
    {
        realloc_cur(stmt, op->p1 + 1);
    }
    // chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    chidb_dbm_cursor_t *cursor;
    chidb_Cursor_open(&cursor, CURSOR_WRITE, stmt->db->bt, stmt->reg[op->p2].value.i, op->p3);
    stmt->cursors[op->p1] = *cursor;
    return CHIDB_OK;
}

int chidb_dbm_op_Close(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return chidb_Cursor_freeCursor(stmt->cursors + op->p1);
}

int chidb_dbm_op_Rewind(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    int try_rewind = chidb_Cursor_rewind(cursor);
    if (try_rewind == CHIDB_OK)
    {
        return CHIDB_OK;
    }
    else if (try_rewind == CHIDB_CURSOR_EMPTY_BTREE)
    {
        stmt->pc = op->p2;
        return CHIDB_OK;
    }
    else
    {
        return try_rewind;
    }
}

int chidb_dbm_op_Next(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    int try_next = chidb_Cursor_next(cursor);
    if (try_next == CHIDB_CURSOR_LAST_ENTRY)
    {
        chilog(INFO, "Cursor %d at end, doing nothing", op->p1);
    }
    else
    {
        stmt->pc = op->p2;
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Prev(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    int try_prev = chidb_Cursor_prev(cursor);
    if (try_prev == CHIDB_CURSOR_FIRST_ENTRY)
    {
    }
    else
    {
        stmt->pc = op->p2;
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Seek(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    int try_seek = chidb_Cursor_seek(cursor, stmt->reg[op->p3].value.i);
    if (try_seek == CHIDB_ENOTFOUND)
    {
        chilog(DEBUG, "Seek failed, jumping to %d.", op->p2);
        stmt->pc = op->p2;
    }
    return CHIDB_OK;
}

int chidb_dbm_op_SeekGt(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    int try_seek = chidb_Cursor_seekGt(cursor, stmt->reg[op->p3].value.i);
    if (try_seek == CHIDB_CURSOR_LAST_ENTRY)
    {
        stmt->pc = op->p2;
    }
    // stmt->pc = op->p2;
    return CHIDB_OK;
}

int chidb_dbm_op_SeekGe(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    int try_seek = chidb_Cursor_seekGte(cursor, stmt->reg[op->p3].value.i);
    if (try_seek == CHIDB_CURSOR_LAST_ENTRY)
    {
        stmt->pc = op->p2;
    }
    // stmt->pc = op->p2;
    return CHIDB_OK;
}

int chidb_dbm_op_SeekLt(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    int try_seek = chidb_Cursor_seekLt(cursor, stmt->reg[op->p3].value.i);
    if (try_seek == CHIDB_CURSOR_FIRST_ENTRY)
    {
        stmt->pc = op->p2;
    }
    // stmt->pc = op->p2;
    return CHIDB_OK;
}

int chidb_dbm_op_SeekLe(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    int try_seek = chidb_Cursor_seekLte(cursor, stmt->reg[op->p3].value.i);
    if (try_seek == CHIDB_CURSOR_FIRST_ENTRY)
    {
        stmt->pc = op->p2;
    }
    return CHIDB_OK;
}

// size in bytes
static int col_size(int col_type)
{
    if (col_type == 0)
    {
        return 0;
    }
    else if (col_type == 1)
    {
        return 1;
    }
    else if (col_type == 2)
    {
        return 2;
    }
    else if (col_type == 4)
    {
        return 4;
    }
    else
    {
        return (col_type - 13) / 2;
    }
}

int chidb_dbm_op_Column(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if (op->p3 >= stmt->nReg)
    {
        realloc_reg(stmt, op->p3 + 1);
    }
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    if (cursor->col_n <= op->p2)
    {
        chilog(WARNING, "col_n %d col # %d", cursor->col_n, op->p2);
        return CHIDB_ECANTOPEN;
    }
    BTreeCell cell;
    chidb_Cursor_get(cursor, &cell);
    uint8_t *data = cell.fields.tableLeaf.data;
    uint8_t *ptr = data;
    uint32_t type;
    uint32_t offset_to_col = 0;
    getRecordCol(data, op->p2, &type, &offset_to_col);
    ptr += offset_to_col;
    chidb_dbm_register_t *reg = stmt->reg + op->p3;
    if (type == 0)
    {
        reg->type = REG_NULL;
    }
    else if (type == 1 || type == 2 || type == 4)
    {
        reg->type = REG_INT32;
        if (type == 1)
        {
            reg->value.i = *ptr;
        }
        else if (type == 2)
        {
            reg->value.i = get2byte(ptr);
        }
        else if (type == 4)
        {
            reg->value.i = get4byte(ptr);
        }
        chilog(DEBUG, "setting col %d, in reg %d", reg->value.i, op->p3);
    }
    else
    {
        reg->type = REG_STRING;
        int str_size = col_size(type);
        reg->value.s = malloc(str_size + 1);
        memcpy(reg->value.s, ptr, str_size);
        reg->value.s[str_size] = '\0';
        chilog(DEBUG, "setting col %s, in reg %d", reg->value.s, op->p3);
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Key(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if (op->p2 >= stmt->nReg)
    {
        realloc_reg(stmt, op->p2 + 1);
    }
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    cursor_node_entry *entry = cursor->node_entries + (cursor->nNodes - 1);
    BTreeNode *btn = entry->node;
    BTreeCell cell;
    for (int i = 0; i < btn->n_cells; i++)
    {
        chidb_Btree_getCell(btn, i, &cell);
        if (cell.key == cursor->curr_key)
        {
            chilog(DEBUG, "Op_Key: Found key %d", cell.key);
            break;
        }
    }
    chidb_dbm_register_t *reg = stmt->reg + op->p2;
    reg->type = REG_INT32;
    reg->value.i = cell.key;
    return CHIDB_OK;
}

int chidb_dbm_op_Integer(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if (op->p2 >= stmt->nReg)
    {
        realloc_reg(stmt, op->p2 + 1);
    }
    stmt->reg[op->p2].type = REG_INT32;
    stmt->reg[op->p2].value.i = op->p1;
    return CHIDB_OK;
}

int chidb_dbm_op_String(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if (op->p2 >= stmt->nReg)
    {
        realloc_reg(stmt, op->p2 + 1);
    }
    stmt->reg[op->p2].type = REG_STRING;
    stmt->reg[op->p2].value.s = malloc(op->p1 + 1);
    memcpy(stmt->reg[op->p2].value.s, op->p4, op->p1);
    stmt->reg[op->p2].value.s[op->p1] = '\0';
    return CHIDB_OK;
}

int chidb_dbm_op_Null(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if (op->p2 >= stmt->nReg)
    {
        realloc_reg(stmt, op->p2 + 1);
    }
    stmt->reg[op->p2].type = REG_NULL;
    return CHIDB_OK;
}

// function for debugging
static void logreg(chidb_stmt *stmt, int n)
{
    chidb_dbm_register_t *reg = stmt->reg + n;
    chilog(DEBUG, "REGISTER %d, %d type", n, reg->type);
    if (reg->type == REG_INT32)
    {
        chilog(DEBUG, "REG VALUE %d", reg->value.i);
    }
    else if (reg->type == REG_STRING)
    {
        chilog(DEBUG, "REG VALUE %s", reg->value.s);
    }
    else if (reg->type == REG_NULL)
    {
        chilog(DEBUG, "REG VALUE NULL");
    }
}

int chidb_dbm_op_ResultRow(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    stmt->startRR = op->p1;
    stmt->nRR = op->p2;
    stmt->nCols = op->p2;
    stmt->cols = malloc(op->p2 * sizeof(char *));
    return CHIDB_ROW;
}

int chidb_dbm_op_MakeRecord(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    // create data
    chilog(DEBUG, "Make record: %d %d %d %s", op->p1, op->p2, op->p3, op->p4);
    uint8_t header_size = 1;
    uint32_t record_size = 1;
    uint32_t types[op->p2];
    for (int i = 0; i < op->p2; i++)
    {
        chidb_dbm_register_t *curr_reg = stmt->reg + op->p1 + i;
        uint32_t header_field_size;
        uint32_t data_size;
        if (curr_reg->type == REG_NULL)
        {
            types[i] = 0;
            header_field_size = 1;
            data_size = 0;
        }
        else if (curr_reg->type == REG_INT32)
        {
            types[i] = 4;
            header_field_size = 1;
            data_size = 4;
        }
        else if (curr_reg->type == REG_STRING)
        {
            data_size = strlen(curr_reg->value.s);
            types[i] = 2 * data_size + 13;
            header_field_size = 4;
        }
        else
        {
            chilog(CRITICAL, "UNEXPECTED TYPE ENCOUNTERED");
        }
        header_size += header_field_size;
        record_size += (header_field_size + data_size);
    }
    uint8_t *data = malloc(record_size);
    uint8_t *header_ptr = data;
    uint8_t *data_ptr = data + header_size;
    *header_ptr = header_size;
    header_ptr += 1;
    for (int i = 0; i < op->p2; i++)
    {
        chidb_dbm_register_t *curr_reg = stmt->reg + op->p1 + i;
        if (types[i] == 0 || types[i] == 4)
        {
            *header_ptr = types[i];
            header_ptr += 1;
            if (types[i] == 4)
            {
                put4byte(data_ptr, curr_reg->value.i);
                data_ptr += 4;
            }
        }
        else
        {
            putVarint32(header_ptr, types[i]);
            header_ptr += 4;
            uint32_t len = (types[i] - 13) / 2;
            memcpy(data_ptr, curr_reg->value.s, len);
            data_ptr += len;
        }
        logreg(stmt, op->p1 + i);
    }
    if (op->p3 >= stmt->nReg)
    {
        realloc_reg(stmt, op->p3 + 1);
    }
    chidb_dbm_register_t *reg = stmt->reg + op->p3;
    reg->type = REG_BINARY;
    reg->value.bin.bytes = data;
    reg->value.bin.nbytes = record_size;
    return CHIDB_OK;
}

int chidb_dbm_op_Insert(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    chidb_key_t key = cursor->curr_key;
    chidb_dbm_register_t *r1 = stmt->reg + op->p2;
    chidb_dbm_register_t *r2 = stmt->reg + op->p3;
    chilog(DEBUG, "Trying to insert key %d in root page %d, from data in reg %d.", r2->value.i, cursor->root_page_n, op->p2);
    int try_insert = chidb_Btree_insertInTable(cursor->bt, cursor->root_page_n, r2->value.i, r1->value.bin.bytes, r1->value.bin.nbytes);
    if (try_insert != CHIDB_OK)
    {
        chilog(DEBUG, "Insertion failed.");
        return try_insert;
    }
    chidb_Cursor_rewind(cursor);
    chidb_Cursor_setKey(cursor, key, 0);
    return CHIDB_OK;
}

int chidb_dbm_op_Eq(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_register_t r1 = stmt->reg[op->p1];
    chidb_dbm_register_t r2 = stmt->reg[op->p3];
    if (r1.type == REG_INT32)
    {
        if (r1.value.i == r2.value.i)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_STRING)
    {
        if (strcmp(r1.value.s, r2.value.s) == 0)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_BINARY)
    {
        if (r1.value.bin.nbytes == r2.value.bin.nbytes &&
            memcmp(r1.value.bin.bytes, r2.value.bin.bytes, r1.value.bin.nbytes) == 0)
        {
            stmt->pc = op->p2;
        }
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Ne(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_register_t r1 = stmt->reg[op->p1];
    chidb_dbm_register_t r2 = stmt->reg[op->p3];
    if (r1.type == REG_INT32)
    {
        if (r1.value.i != r2.value.i)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_STRING)
    {
        if (strcmp(r1.value.s, r2.value.s) != 0)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_BINARY)
    {
        if (r1.value.bin.nbytes == r2.value.bin.nbytes)
        {
            if (memcmp(r2.value.bin.bytes, r1.value.bin.bytes, r1.value.bin.nbytes) != 0)
            {
                stmt->pc = op->p2;
            }
        }
        else
        {
            stmt->pc = op->p2;
        }
    }
    if (r2.type != r1.type)
    {
        stmt->pc = op->p2;
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Lt(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_register_t r1 = stmt->reg[op->p1];
    chidb_dbm_register_t r2 = stmt->reg[op->p3];
    if (r1.type == REG_INT32)
    {
        if (r2.value.i < r1.value.i)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_STRING)
    {
        if (strcmp(r2.value.s, r1.value.s) < 0)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_BINARY)
    {
        if (r2.value.bin.nbytes == r1.value.bin.nbytes &&
            memcmp(r2.value.bin.bytes, r1.value.bin.bytes, r2.value.bin.nbytes) < 0)
        {
            stmt->pc = op->p2;
        }
        else if (r2.value.bin.nbytes < r1.value.bin.nbytes)
        {
            stmt->pc = op->p2;
        }
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Le(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_register_t r1 = stmt->reg[op->p1];
    chidb_dbm_register_t r2 = stmt->reg[op->p3];
    if (r1.type == REG_INT32)
    {
        if (r2.value.i <= r1.value.i)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_STRING)
    {
        if (strcmp(r2.value.s, r1.value.s) <= 0)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_BINARY)
    {
        if (r2.value.bin.nbytes == r1.value.bin.nbytes &&
            memcmp(r2.value.bin.bytes, r1.value.bin.bytes, r2.value.bin.nbytes) <= 0)
        {
            stmt->pc = op->p2;
        }
        if (r2.value.bin.nbytes < r1.value.bin.nbytes)
        {
            stmt->pc = op->p2;
        }
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Gt(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_register_t r1 = stmt->reg[op->p1];
    chidb_dbm_register_t r2 = stmt->reg[op->p3];
    if (r1.type == REG_INT32)
    {
        if (r2.value.i > r1.value.i)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_STRING)
    {
        if (strcmp(r2.value.s, r1.value.s) > 0)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_BINARY)
    {
        if (r2.value.bin.nbytes == r1.value.bin.nbytes &&
            memcmp(r2.value.bin.bytes, r1.value.bin.bytes, r2.value.bin.nbytes) > 0)
        {
            stmt->pc = op->p2;
        }
        else if (r2.value.bin.nbytes > r1.value.bin.nbytes)
        {
            stmt->pc = op->p2;
        }
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Ge(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    chidb_dbm_register_t r1 = stmt->reg[op->p1];
    chidb_dbm_register_t r2 = stmt->reg[op->p3];
    if (r1.type == REG_INT32)
    {
        if (r2.value.i >= r1.value.i)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_STRING)
    {
        if (strcmp(r2.value.s, r1.value.s) >= 0)
        {
            stmt->pc = op->p2;
        }
    }
    else if (r1.type == REG_BINARY)
    {
        if (r2.value.bin.nbytes == r1.value.bin.nbytes &&
            memcmp(r2.value.bin.bytes, r1.value.bin.bytes, r2.value.bin.nbytes) >= 0)
        {
            stmt->pc = op->p2;
        }
        if (r2.value.bin.nbytes > r1.value.bin.nbytes)
        {
            stmt->pc = op->p2;
        }
    }
    return CHIDB_OK;
}

/* IdxGt p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) > k, jump
 */
int chidb_dbm_op_IdxGt(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    if (cursor->curr_key > stmt->reg[op->p3].value.i)
    {
        stmt->pc = op->p2;
    }
    // fprintf(stderr, "todo: chidb_dbm_op_IdxGt\n");
    // exit(1);
    return CHIDB_OK;
}

/* IdxGe p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) >= k, jump
 */
int chidb_dbm_op_IdxGe(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    if (cursor->curr_key >= stmt->reg[op->p3].value.i)
    {
        stmt->pc = op->p2;
    }
    return CHIDB_OK;
}

/* IdxLt p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) < k, jump
 */
int chidb_dbm_op_IdxLt(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    if (cursor->curr_key < stmt->reg[op->p3].value.i)
    {
        stmt->pc = op->p2;
    }
    return CHIDB_OK;
}

/* IdxLe p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) <= k, jump
 */
int chidb_dbm_op_IdxLe(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    if (cursor->curr_key <= stmt->reg[op->p3].value.i)
    {
        stmt->pc = op->p2;
    }
    return CHIDB_OK;
}

/* IdxPKey p1 p2 * *
 *
 * p1: cursor
 * p2: register
 *
 * store pkey from (cell at cursor p1) in (register at p2)
 */
int chidb_dbm_op_IdxPKey(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    if (stmt->nReg <= op->p2)
    {
        realloc_reg(stmt, op->p2 + 1);
    }
    chidb_dbm_register_t *reg = stmt->reg + op->p2;
    reg->type = REG_INT32;

    cursor_node_entry *entry = cursor->node_entries + cursor->nNodes - 1;
    BTreeCell cell;
    chidb_Btree_getCell(entry->node, entry->ncell, &cell);
    if (cell.type == PGTYPE_INDEX_INTERNAL)
    {
        reg->value.i = cell.fields.indexInternal.keyPk;
    }
    else if (cell.type == PGTYPE_INDEX_LEAF)
    {
        reg->value.i = cell.fields.indexLeaf.keyPk;
    }
    else
    {
        chilog(CRITICAL, "UNEXPECTED CELL INDEX TYPE");
    }
    return CHIDB_OK;
}

/* IdxInsert p1 p2 p3 *
 *
 * p1: cursor
 * p2: register containing IdxKey
 * p2: register containing PKey
 *
 * add new (IdkKey,PKey) entry in index BTree pointed at by cursor at p1
 */
int chidb_dbm_op_IdxInsert(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    chidb_dbm_cursor_t *cursor = stmt->cursors + op->p1;
    chidb_key_t key = cursor->curr_key;
    chidb_key_t idxKey = stmt->reg[op->p2].value.i;
    chidb_key_t pKey = stmt->reg[op->p3].value.i;

    int try_insert = chidb_Btree_insertInIndex(cursor->bt, cursor->root_page_n, idxKey, pKey);
    if (try_insert != CHIDB_OK)
    {
        chilog(WARNING, "Btree index insert returned with code %d", try_insert);
        return try_insert;
    }
    chidb_Cursor_rewind(cursor);
    chidb_Cursor_setKey(cursor, key, 0);
    return CHIDB_OK;
}

int chidb_dbm_op_CreateTable(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    npage_t new_npage;
    chidb_Btree_newNode(stmt->db->bt, &new_npage, PGTYPE_TABLE_LEAF);
    if (stmt->nReg <= op->p1)
    {
        realloc_reg(stmt, op->p1 + 1);
    }
    chidb_dbm_register_t *reg = stmt->reg + op->p1;
    reg->type = REG_INT32;
    reg->value.i = new_npage;
    return CHIDB_OK;
}

int chidb_dbm_op_CreateIndex(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    npage_t new_npage;
    chidb_Btree_newNode(stmt->db->bt, &new_npage, PGTYPE_INDEX_LEAF);
    if (stmt->nReg <= op->p1)
    {
        realloc_reg(stmt, op->p1 + 1);
    }
    chidb_dbm_register_t *reg = stmt->reg + op->p1;
    reg->type = REG_INT32;
    reg->value.i = new_npage;
    return CHIDB_OK;
}

int chidb_dbm_op_Copy(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

int chidb_dbm_op_SCopy(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

int chidb_dbm_op_Halt(chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    // if (op->p1 != 0)
    // {
    //     stmt->pc = stmt->nOps;
    // }
    stmt->pc = stmt->nOps;
    return CHIDB_OK;
}
