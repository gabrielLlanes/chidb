/*
 *  chidb - a didactic relational database management system
 *
 * This module contains functions to manipulate a B-Tree file. In this context,
 * "BTree" refers not to a single B-Tree but to a "file of B-Trees" ("chidb
 * file" and "file of B-Trees" are essentially equivalent terms).
 *
 * However, this module does *not* read or write to the database file directly.
 * All read/write operations must be done through the pager module.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <chidb/log.h>
#include "chidbInt.h"
#include "btree.h"
#include "record.h"
#include "pager.h"
#include "util.h"
#include <math.h>

void reverse_endian(uint8_t *to, uint8_t *from, int n)
{
    for (int i = 0; i < n; i++)
    {
        to[i] = from[n - 1 - i];
    }
}

static const uint8_t DEFAULT_FILE_HEADER[100] = {
    'S', 'Q', 'L', 'i', 't', 'e', ' ', 'f', 'o', 'r', 'm', 'a', 't', ' ', '3', '\0',
    0x04, 0x00, 0x01, 0x01, 0x00, 0x40, 0x20, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
    0x00, 0x00, 0x4E, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00};

/* Open a B-Tree file
 *
 * This function opens a database file and verifies that the file
 * header is correct. If the file is empty (which will happen
 * if the pager is given a filename for a file that does not exist)
 * then this function will (1) initialize the file header using
 * the default page size and (2) create an empty table leaf node
 * in page 1.
 *
 * Parameters
 * - filename: Database file (might not exist)
 * - db: A chidb struct. Its bt field must be set to the newly
 *       created BTree.
 * - bt: An out parameter. Used to return a pointer to the
 *       newly created BTree.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECORRUPTHEADER: Database file contains an invalid header
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt)
{
    int exists;
    // first determine whether the file does not exist/is empty,
    // so that we may initialize the database with a file header and
    // an initial page.
    FILE *fp = fopen(filename, "r+");
    if (fp == NULL)
    {
        exists = 0;
    }
    else
    {
        fseek(fp, 0, SEEK_END);
        int file_size = ftell(fp);
        if (file_size == 0)
        {
            exists = 0;
        }
        else
        {
            exists = 1;
        }
        fclose(fp);
    }
    /* Your code goes here */
    Pager *pager;
    int try_open = chidb_Pager_open(&pager, filename);
    if (try_open == CHIDB_ENOMEM)
    {
        return CHIDB_ENOMEM;
    }
    else if (try_open == CHIDB_EIO)
    {
        return CHIDB_EIO;
    }
    BTree *btree = (BTree *)malloc(sizeof(BTree));
    btree->db = db;
    btree->pager = pager;
    db->bt = btree;
    *bt = btree;

    if (exists == 0)
    {
        npage_t npage;
        pager->n_pages = 0;
        pager->page_size = DEFAULT_PAGE_SIZE;
        chidb_Pager_allocatePage(pager, &npage);
        MemPage *page;
        int try_read_page = chidb_Pager_readPage(pager, 1, &page);
        if (try_read_page == CHIDB_ENOMEM || try_read_page == CHIDB_EIO)
        {
            return try_read_page;
        }
        memcpy(page->data, DEFAULT_FILE_HEADER, 100);
        int try_write_header = chidb_Pager_writePage(pager, page);
        free(page->data);
        free(page);
        if (try_write_header == CHIDB_EPAGENO || try_write_header == CHIDB_EIO)
        {
            return try_write_header;
        }
        int try_init = chidb_Btree_initEmptyNode(btree, 1, PGTYPE_TABLE_LEAF);
        if (try_init == CHIDB_ENOMEM || try_init == CHIDB_EIO)
        {
            return try_init;
        }
        return CHIDB_OK;
    }

    uint8_t header[100];
    int try_read_header = chidb_Pager_readHeader(pager, header);
    if (try_read_header == CHIDB_NOHEADER)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    uint8_t *ptr = header;

    uint8_t header_top[16];
    uint8_t head_top_expected[16] = {'S', 'Q', 'L', 'i', 't', 'e', ' ', 'f', 'o', 'r', 'm', 'a', 't', ' ', '3', '\0'};
    memcpy(header_top, ptr, 16);
    if (strcmp((const char *)header_top, (const char *)head_top_expected) != 0)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    ptr += 16;

    uint16_t page_size = get2byte(ptr);
    if (page_size < 0)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    chidb_Pager_setPageSize(pager, page_size);
    ptr += 2;

    uint8_t bytes_18_thru_23[6];
    uint8_t expected_18_thru_23[] = {0x01, 0x01, 0x00, 0x40, 0x20, 0x20};
    memcpy(bytes_18_thru_23, ptr, 6);
    for (int i = 0; i < 6; i++)
    {
        if (bytes_18_thru_23[i] != expected_18_thru_23[i])
        {
            return CHIDB_ECORRUPTHEADER;
        }
    }
    ptr += 6;

    uint32_t file_change_counter = get4byte(ptr);
    if (file_change_counter != 0)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    ptr += 4;
    ptr += 4;

    uint64_t bytes_32_thru_39;
    reverse_endian((uint8_t *)&bytes_32_thru_39, ptr, 8);
    if (bytes_32_thru_39 != 0)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    ptr += 8;

    uint32_t schema_version = get4byte(ptr);
    if (schema_version != 0)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    ptr += 4;

    uint32_t bytes_44_thru_47 = get4byte(ptr);
    if (bytes_44_thru_47 != 0x00000001)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    ptr += 4;

    uint32_t page_cache_size = get4byte(ptr);
    if (page_cache_size != 20000)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    ptr += 4;

    uint64_t bytes_52_thru_59;
    reverse_endian((uint8_t *)&bytes_52_thru_59, ptr, 8);
    if (bytes_52_thru_59 != 0x0000000000000001)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    ptr += 4;
    ptr += 4;

    uint32_t user_cookie = get4byte(ptr);
    if (user_cookie != 0)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    ptr += 4;

    uint32_t bytes_64_thru_67 = get4byte(ptr);
    if (bytes_64_thru_67 != 0)
    {
        return CHIDB_ECORRUPTHEADER;
    }
    return CHIDB_OK;
}

/* Close a B-Tree file
 *
 * This function closes a database file, freeing any resource
 * used in memory, such as the pager.
 *
 * Parameters
 * - bt: B-Tree file to close
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_close(BTree *bt)
{
    /* Your code goes here */
    chidb_Pager_close(bt->pager);
    free(bt);

    return CHIDB_OK;
}

/* Loads a B-Tree node from disk
 *
 * Reads a B-Tree node from a page in the disk. All the information regarding
 * the node is stored in a BTreeNode struct (see header file for more details
 * on this struct). *This is the only function that can allocate memory for
 * a BTreeNode struct*. Always use chidb_Btree_freeMemNode to free the memory
 * allocated for a BTreeNode (do not use free() directly on a BTreeNode variable)
 * Any changes made to a BTreeNode variable will not be effective in the database
 * until chidb_Btree_writeNode is called on that BTreeNode.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Page of node to load
 * - btn: Out parameter. Used to return a pointer to newly creater BTreeNode
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int counting = 0;
int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **btn)
{
    /* Your code goes here */
    BTreeNode *_btn = (BTreeNode *)malloc(sizeof(BTreeNode));
    MemPage *page;
    int try_read_page = chidb_Pager_readPage(bt->pager, npage, &page);
    if (try_read_page == CHIDB_EPAGENO || try_read_page == CHIDB_EIO || try_read_page == CHIDB_ENOMEM)
    {
        return try_read_page;
    }
    _btn->page = page;
    uint8_t *data = page->data;
    if (npage == 1)
    {
        // the page header of page 1 is 100 bytes from the start of the page, as the first 100 bytes are the file header.
        data += 100;
    }
    _btn->type = *data;
    data += 1;
    _btn->free_offset = get2byte(data);
    data += 2;
    _btn->n_cells = get2byte(data);
    data += 2;
    _btn->cells_offset = get2byte(data);
    data += 2;
    data += 1;
    uint8_t type = _btn->type;
    chilog(DEBUG, "Btree %d, %d free offset, %d cells, %d cells offset %d cell type",
           npage, _btn->free_offset, _btn->n_cells, _btn->cells_offset, type);
    // 0x05: internal table page, 0x02: internal index page
    if (type == PGTYPE_INDEX_INTERNAL || type == PGTYPE_TABLE_INTERNAL)
    {
        _btn->right_page = get4byte(data);
        data += 4;
        _btn->celloffset_array = data;
    }
    else if (type == PGTYPE_INDEX_LEAF || type == PGTYPE_TABLE_LEAF)
    {
        _btn->celloffset_array = data;
    }
    *btn = _btn;
    return CHIDB_OK;
}

/* Frees the memory allocated to an in-memory B-Tree node
 *
 * Frees the memory allocated to an in-memory B-Tree node, and
 * the in-memory page returned by the pages (stored in the
 * "page" field of BTreeNode)
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to free
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn)
{
    /* Your code goes here */
    chidb_Pager_releaseMemPage(bt->pager, btn->page);
    free(btn);
    return CHIDB_OK;
}

// utility function for generating an initial header, given some initial information
static uint8_t init_header(uint8_t type, uint16_t page_size, npage_t npage, uint8_t *header)
{
    uint8_t header_size = 0;
    uint16_t free_offset = 0;
    uint16_t n_cells = 0;
    uint16_t cells_offset = page_size;
    uint8_t _zero = 0;
    if (type == PGTYPE_INDEX_INTERNAL || type == PGTYPE_TABLE_INTERNAL)
    {
        header_size = 12;
    }
    else if (type == PGTYPE_INDEX_LEAF || type == PGTYPE_TABLE_LEAF)
    {
        header_size = 8;
    }
    else
    {
        return CHIDB_ECORRUPT;
    }
    free_offset += header_size;
    if (npage == 1)
    {
        free_offset += 100;
    }
    uint8_t *ptr = header;

    *ptr = type;
    ptr += 1;
    put2byte_le(ptr, free_offset);
    ptr += 2;
    put2byte_le(ptr, n_cells);
    ptr += 2;
    put2byte_le(ptr, cells_offset);
    ptr += 2;
    *ptr = _zero;
    ptr += 1;
    if (type == PGTYPE_INDEX_INTERNAL || type == PGTYPE_TABLE_INTERNAL)
    {
        put4byte(ptr, npage);
    }

    return header_size;
}

/* Create a new B-Tree node
 *
 * Allocates a new page in the file and initializes it as a B-Tree node.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Out parameter. Returns the number of the page that
 *          was allocated.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type)
{
    /* Your code goes here */
    chidb_Pager_allocatePage(bt->pager, npage);
    return chidb_Btree_initEmptyNode(bt, *npage, type);
}

/* Initialize a B-Tree node
 *
 * Initializes a database page to contain an empty B-Tree node. The
 * database page is assumed to exist and to have been already allocated
 * by the pager.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Database page where the node will be created.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type)
{
    /* Your code goes here */
    MemPage *page;
    int try_read_page = chidb_Pager_readPage(bt->pager, npage, &page);
    if (try_read_page == CHIDB_EPAGENO || try_read_page == CHIDB_EIO || try_read_page == CHIDB_ENOMEM)
    {
        return try_read_page;
    }
    uint8_t header[12];
    int header_size = init_header(type, bt->pager->page_size, npage, header);
    memcpy(page->data + (npage == 1 ? 100 : 0), header, header_size);
    return chidb_Pager_writePage(bt->pager, page);
}

/* Write an in-memory B-Tree node to disk
 *
 * Writes an in-memory B-Tree node to disk. To do this, we need to update
 * the in-memory page according to the chidb page format. Since the cell
 * offset array and the cells themselves are modified directly on the
 * page, the only thing to do is to store the values of "type",
 * "free_offset", "n_cells", "cells_offset" and "right_page" in the
 * in-memory page.
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to write to disk
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_writeNode(BTree *bt, BTreeNode *btn)
{
    /* Your code goes here */

    // make sure that any updated header information in the struct
    // is updated in the in-memory page before writing the entire page
    // to disk. The modification of the cell offset array and free space,
    // the addition of any new cells, and the corresponding updating of
    // information in the fields of the BTreeNode struct will occur in
    // functions such as chidb_Btree_insertCell
    uint8_t *ptr = btn->page->data;
    if (btn->page->npage == 1)
    {
        ptr += 100;
    }
    *ptr = btn->type;
    ptr += 1;
    put2byte_le(ptr, btn->free_offset);
    ptr += 2;
    put2byte_le(ptr, btn->n_cells);
    ptr += 2;
    put2byte_le(ptr, btn->cells_offset);
    ptr += 2;
    uint8_t _zero = 0;
    *ptr = _zero;
    ptr += 1;
    if (btn->type == PGTYPE_INDEX_INTERNAL || btn->type == PGTYPE_TABLE_INTERNAL)
    {
        put4byte(ptr, btn->right_page);
    }
    return chidb_Pager_writePage(bt->pager, btn->page);
}

/* Read the contents of a cell
 *
 * Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
 * This involves the following:
 *  1. Find out the offset of the requested cell.
 *  2. Read the cell from the in-memory page, and parse its
 *     contents (refer to The chidb File Format document for
 *     the format of cells).
 *
 * Parameters
 * - btn: BTreeNode where cell is contained
 * - ncell: Cell number
 * - cell: BTreeCell where contents must be stored.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    /* Your code goes here */
    MemPage *page = btn->page;
    uint8_t *ptr = btn->celloffset_array;
    if (ncell < 0 || ncell >= btn->n_cells)
    {
        return CHIDB_ECELLNO;
    }
    ptr += 2 * (ncell); // now pointing to the ncell entry of the offset array.
    uint16_t ncell_offset = get2byte(ptr);
    ptr = page->data + ncell_offset; // relative to start of page
    uint8_t type = btn->type;
    cell->type = btn->type;
    if (type == PGTYPE_TABLE_INTERNAL)
    {
        (cell->fields).tableInternal.child_page = get4byte(ptr);
        getVarint32(ptr + 4, &(cell->key));
    }
    else if (type == PGTYPE_TABLE_LEAF)
    {
        getVarint32(ptr, &((cell->fields).tableLeaf.data_size));
        getVarint32(ptr + 4, &(cell->key));
        (cell->fields).tableLeaf.data = ptr + 8;
    }
    else if (type == PGTYPE_INDEX_INTERNAL)
    {
        (cell->fields).indexInternal.child_page = get4byte(ptr);
        cell->key = get4byte(ptr + 8);
        (cell->fields).indexInternal.keyPk = get4byte(ptr + 12);
    }
    else if (type == PGTYPE_INDEX_LEAF)
    {
        cell->key = get4byte(ptr + 4);
        (cell->fields).indexLeaf.keyPk = get4byte(ptr + 8);
    }
    else
    {
        chilog(CRITICAL, "INVALID CELL TYPE: FATAL.");
        exit(1);
    }
    return CHIDB_OK;
}

/* Insert a new cell into a B-Tree node
 *
 * Inserts a new cell into a B-Tree node at a specified position ncell.
 * This involves the following:
 *  1. Add the cell at the top of the cell area. This involves "translating"
 *     the BTreeCell into the chidb format (refer to The chidb File Format
 *     document for the format of cells).
 *  2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
 *  3. Modify the cell offset array so that all values in positions >= ncell
 *     are shifted one position forward in the array. Then, set the value of
 *     position ncell to be the offset of the newly added cell.
 *
 * This function assumes that there is enough space for this cell in this node.
 *
 * Parameters
 * - btn: BTreeNode to insert cell in
 * - ncell: Cell number
 * - cell: BTreeCell to insert.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    /* Your code goes here */

    //  existing cells range from 0 thru ncells - 1, for a total of ncells.
    //  valid range of cells to insert at is 0 thru ncells. if 0 <= ncell <= ncells - 1, then
    //  cell offset array entries at position ncell thru ncells - 1 are shifted forwards
    //  by 2 bytes. If ncell = ncells, then simply add an entry at the end of the cell offset array,
    //  which is current start of free space.
    ncell_t n_cells = btn->n_cells;
    if (ncell > n_cells || ncell < 0)
    {
        return CHIDB_ECELLNO;
    }
    else
    {
        /*need to make the following changes to the header:
        1: increment free_offset by 2.
        2: increment n_cells by 1.
        3: subtract the size of the cell from cells_offset.
        */
        uint8_t type = btn->type;
        if (ncell < n_cells)
        {
            for (int i = n_cells - 1; i >= ncell; i--)
            {
                uint16_t ith_cell_offset_value = get2byte((btn->celloffset_array) + 2 * i);
                put2byte((btn->celloffset_array) + (2 * i + 2), ith_cell_offset_value);
            }
            // the value of the cell offset entry for the new cell will be determined upon inspecting the
            // field values of the cell.
        }

        uint8_t *new_cell_ptr = btn->page->data + btn->cells_offset;
        uint16_t cell_size;
        if (type == PGTYPE_TABLE_INTERNAL)
        {
            cell_size = 8;
            new_cell_ptr -= 8;
            put4byte(new_cell_ptr, cell->fields.tableInternal.child_page);
            putVarint32(new_cell_ptr + 4, cell->key);
        }
        else if (type == PGTYPE_TABLE_LEAF)
        {
            uint32_t record_size = cell->fields.tableLeaf.data_size;
            cell_size = 8 + record_size;
            new_cell_ptr -= cell_size;
            putVarint32(new_cell_ptr, record_size);
            putVarint32(new_cell_ptr + 4, cell->key);
            memcpy(new_cell_ptr + 8, cell->fields.tableLeaf.data, record_size);
        }
        else if (type == PGTYPE_INDEX_INTERNAL)
        {
            cell_size = 16;
            new_cell_ptr -= 16;
            put4byte(new_cell_ptr, cell->fields.indexInternal.child_page);
            put4byte(new_cell_ptr + 4, 0x0B030404);
            put4byte(new_cell_ptr + 8, cell->key);
            put4byte(new_cell_ptr + 12, cell->fields.indexInternal.keyPk);
        }
        else if (type == PGTYPE_INDEX_LEAF)
        {
            cell_size = 12;
            new_cell_ptr -= 12;
            put4byte(new_cell_ptr, 0x0B030404);
            put4byte(new_cell_ptr + 4, cell->key);
            put4byte(new_cell_ptr + 8, cell->fields.indexLeaf.keyPk);
        }
        else
        {
            return CHIDB_ECORRUPT;
        }
        put2byte((btn->celloffset_array) + 2 * ncell, btn->cells_offset - cell_size);
        btn->free_offset += 2;
        btn->n_cells += 1;
        btn->cells_offset -= cell_size;
    }
    return CHIDB_OK;
}

/* Find an entry in a table B-Tree
 *
 * Finds the data associated for a given key in a table B-Tree
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want search in
 * - key: Entry key
 * - data: Out-parameter where a copy of the data must be stored
 * - size: Out-parameter where the number of bytes of data must be stored
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: No entry with the given key way found
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_find(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t **data, uint16_t *size)
{
    BTreeNode *btn;
    int try_get_node = chidb_Btree_getNodeByPage(bt, nroot, &btn);
    if (try_get_node == CHIDB_ECORRUPT)
    {
        chidb_Btree_freeMemNode(bt, btn);
        return try_get_node;
    }
    uint8_t type = btn->type;
    // descending recursively down through the BTree
    chilog(DEBUG, "BTree rooted at %d: %d cells, page %d, right page %d, type %d. Searching for %d",
           btn->page->npage, btn->n_cells, btn->page->npage, btn->right_page, btn->type, key);
    if (type == PGTYPE_TABLE_INTERNAL || type == PGTYPE_INDEX_INTERNAL)
    {
        for (int i = 0; i < btn->n_cells; i++)
        {
            BTreeCell curr_cell;
            chidb_Btree_getCell(btn, i, &curr_cell);
            if (key <= curr_cell.key)
            {
                chidb_Btree_freeMemNode(bt, btn);
                if (type == PGTYPE_TABLE_INTERNAL)
                {
                    return chidb_Btree_find(bt, curr_cell.fields.tableInternal.child_page, key, data, size);
                }
                else if (type == PGTYPE_INDEX_INTERNAL)
                {
                    if (key == curr_cell.key)
                    {
                        *size = -1;
                        *data = malloc(1);
                        return CHIDB_OK;
                    }
                    return chidb_Btree_find(bt, curr_cell.fields.indexInternal.child_page, key, data, size);
                }
            }
        }
        npage_t right_page = btn->right_page;
        chidb_Btree_freeMemNode(bt, btn);
        return chidb_Btree_find(bt, right_page, key, data, size);
    }
    else if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF)
    {
        for (int i = 0; i < btn->n_cells; i++)
        {
            BTreeCell curr_cell;
            chidb_Btree_getCell(btn, i, &curr_cell);
            if (key == curr_cell.key)
            {
                if (btn->type == PGTYPE_TABLE_LEAF)
                {
                    uint16_t data_size = curr_cell.fields.tableLeaf.data_size;
                    uint8_t *ptr = malloc(data_size);
                    memcpy(ptr, curr_cell.fields.tableLeaf.data, data_size);
                    *data = ptr;
                    *size = data_size;
                    chidb_Btree_freeMemNode(bt, btn);
                    return CHIDB_OK;
                }
                else if (btn->type == PGTYPE_INDEX_LEAF)
                {
                    *size = -1;
                    *data = malloc(1);
                    chilog(DEBUG, "FOUND IN INDEX LEAF");
                    chidb_Btree_freeMemNode(bt, btn);
                    return CHIDB_OK;
                }
            }
        }
    }
    chidb_Btree_freeMemNode(bt, btn);
    return CHIDB_ENOTFOUND;
}

// return 0 if node has space for an extra cell and an entry in the offset array
static int node_has_space(BTreeNode *btn, BTreeCell *cell)
{
    uint16_t cell_size;
    if (cell->type == PGTYPE_TABLE_INTERNAL)
    {
        cell_size = 8;
    }
    else if (cell->type == PGTYPE_TABLE_LEAF)
    {
        cell_size = 8 + cell->fields.tableLeaf.data_size;
    }
    else if (cell->type == PGTYPE_INDEX_INTERNAL)
    {
        cell_size = 16;
    }
    else if (cell->type == PGTYPE_INDEX_LEAF)
    {
        cell_size = 12;
    }
    else
    {
        return CHIDB_ECORRUPT;
    }
    return (btn->cells_offset - btn->free_offset - 2) >= cell_size;
}

/* Insert an entry into a table B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a key and data, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - key: Entry key
 * - data: Pointer to data we want to insert
 * - size: Number of bytes of data
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t *data, uint16_t size)
{
    /* Your code goes here */
    BTreeCell btc;
    btc.key = key;
    btc.type = PGTYPE_TABLE_LEAF;
    btc.fields.tableLeaf.data_size = size;
    btc.fields.tableLeaf.data = data;
    return chidb_Btree_insert(bt, nroot, &btc);
}

/* Insert an entry into an index B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a KeyIdx and a KeyPk, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - keyIdx: See The chidb File Format.
 * - keyPk: See The chidb File Format.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, chidb_key_t keyIdx, chidb_key_t keyPk)
{
    /* Your code goes here */
    chilog(DEBUG, "Entering chidb_Btree_insertInIndex for insertion of key %d into page %d", keyIdx, nroot);
    BTreeCell btc;
    btc.key = keyIdx;
    btc.type = PGTYPE_INDEX_LEAF;
    btc.fields.indexLeaf.keyPk = keyPk;
    return chidb_Btree_insert(bt, nroot, &btc);
}

static void transfer_cells(BTree *bt, BTreeNode *to_node, BTreeNode **from_node, int n, BTreeCell *median_cell)
{
    for (int i = 0; i < n - 1; i++)
    {
        BTreeCell curr_cell;
        chidb_Btree_getCell(*from_node, i, &curr_cell);
        chidb_Btree_insertCell(to_node, i, &curr_cell);
    }
    chidb_Btree_getCell(*from_node, n - 1, median_cell);
    if ((*from_node)->type == PGTYPE_TABLE_LEAF)
    {
        chidb_Btree_insertCell(to_node, n - 1, median_cell);
    }
    else if ((*from_node)->type == PGTYPE_TABLE_INTERNAL)
    {
        to_node->right_page = median_cell->fields.tableInternal.child_page;
    }
    else if ((*from_node)->type == PGTYPE_INDEX_INTERNAL)
    {
        to_node->right_page = median_cell->fields.indexInternal.child_page;
    }
    BTreeNode *new_right_node;
    chidb_Btree_initEmptyNode(bt, (*from_node)->page->npage, (*from_node)->type);
    chidb_Btree_getNodeByPage(bt, (*from_node)->page->npage, &new_right_node);

    for (int i = n; i < (*from_node)->n_cells; i++)
    {
        BTreeCell curr_cell;
        chidb_Btree_getCell(*from_node, i, &curr_cell);
        chidb_Btree_insertCell(new_right_node, i - n, &curr_cell);
    }
    new_right_node->right_page = (*from_node)->right_page;
    *from_node = new_right_node;
}

/* Insert a BTreeCell into a B-Tree
 *
 * The chidb_Btree_insert and chidb_Btree_insertNonFull functions
 * are responsible for inserting new entries into a B-Tree, although
 * chidb_Btree_insertNonFull is the one that actually does the
 * insertion. chidb_Btree_insert, however, first checks if the root
 * has to be split (a splitting operation that is different from
 * splitting any other node). If so, chidb_Btree_split is called
 * before calling chidb_Btree_insertNonFull.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insert(BTree *bt, npage_t nroot, BTreeCell *btc)
{
    uint8_t *data;
    uint16_t size;
    if (chidb_Btree_find(bt, nroot, btc->key, &data, &size) == 0)
    {
        return CHIDB_EDUPLICATE;
    }
    chilog(DEBUG, "Entering chidb_Btree_insert for key %d into page %d", btc->key, nroot);
    /* Your code goes here */
    BTreeNode *root_node;
    int try_get_page = chidb_Btree_getNodeByPage(bt, nroot, &root_node);
    if (try_get_page != CHIDB_OK)
    {
        return try_get_page;
    }
    if (!node_has_space(root_node, btc))
    {
        chilog(INFO, "ROOT, PAGE %d OUT OF SPACE", nroot);
        npage_t new_root_n;
        BTreeNode *new_root_node;
        uint8_t new_root_type = btc->type == PGTYPE_TABLE_LEAF ? PGTYPE_TABLE_INTERNAL : PGTYPE_INDEX_INTERNAL;
        chidb_Btree_newNode(bt, &new_root_n, new_root_type);
        npage_t left_split_n;
        chidb_Btree_split(bt, new_root_n, nroot, 0, &left_split_n);
        chidb_Btree_getNodeByPage(bt, new_root_n, &new_root_node);
        chidb_Btree_getNodeByPage(bt, nroot, &root_node);
        new_root_node->right_page = new_root_n;
        new_root_node->page->npage = nroot;
        root_node->page->npage = new_root_n;

        // need to account for the fact that the first 100 bytes of page 1 is the file header.
        if (nroot == 1)
        {
            uint16_t page_size = bt->pager->page_size;
            uint8_t hold[page_size];
            memcpy(hold, new_root_node->page->data, new_root_node->free_offset);
            memcpy(new_root_node->page->data, root_node->page->data, 100);
            memcpy(new_root_node->page->data + 100, hold, new_root_node->free_offset);
            memcpy(hold, root_node->page->data + 100, root_node->free_offset - 100);
            memcpy(root_node->page->data, hold, root_node->free_offset - 100);
            new_root_node->free_offset += 100;
            new_root_node->celloffset_array += 100;
            root_node->free_offset -= 100;
            root_node->celloffset_array -= 100;
        }
        int a1 = chidb_Btree_writeNode(bt, root_node);
        if (a1 != CHIDB_OK)
        {
            return a1;
        }
        int a2 = chidb_Btree_writeNode(bt, new_root_node);
        if (a2 != CHIDB_OK)
        {
            return a2;
        }
        chidb_Btree_freeMemNode(bt, new_root_node);
    }
    chidb_Btree_freeMemNode(bt, root_node);
    chidb_Btree_insertNonFull(bt, nroot, btc);
    return CHIDB_OK;
}

/* Insert a BTreeCell into a non-full B-Tree node
 *
 * chidb_Btree_insertNonFull inserts a BTreeCell into a node that is
 * assumed not to be full (i.e., does not require splitting). If the
 * node is a leaf node, the cell is directly added in the appropriate
 * position according to its key. If the node is an internal node, the
 * function will determine what child node it must insert it in, and
 * calls itself recursively on that child node. However, before doing so
 * it will check if the child node is full or not. If it is, then it will
 * have to be split first.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *btc)
{
    /* Your code goes here */
    BTreeNode *btn;
    int try_get_page = chidb_Btree_getNodeByPage(bt, npage, &btn);
    if (try_get_page != CHIDB_OK)
    {
        return try_get_page;
    }
    int j;
    BTreeCell curr_cell;
    for (j = 0; j < btn->n_cells; j++)
    {
        chidb_Btree_getCell(btn, j, &curr_cell);
        if (btc->key < curr_cell.key)
        {
            break;
        }
        else if (btc->key == curr_cell.key)
        {
            chidb_Btree_freeMemNode(bt, btn);
            return CHIDB_EDUPLICATE;
        }
    }
    if (btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL)
    {
        npage_t insertion_page;
        if (j == btn->n_cells)
        {
            insertion_page = btn->right_page;
        }
        else
        {
            insertion_page = btn->type == PGTYPE_TABLE_INTERNAL ? curr_cell.fields.tableInternal.child_page : curr_cell.fields.indexInternal.child_page;
        }
        BTreeNode *child_node;
        chidb_Btree_getNodeByPage(bt, insertion_page, &child_node);
        if (!node_has_space(child_node, btc))
        {
            npage_t new_child_n;
            chidb_Btree_split(bt, npage, insertion_page, j, &new_child_n);
            chidb_Btree_getNodeByPage(bt, npage, &btn);
            BTreeCell new_cell_from_split;
            chidb_Btree_getCell(btn, j, &new_cell_from_split);
            if (btc->key <= new_cell_from_split.key)
            {
                insertion_page = btn->type == PGTYPE_TABLE_INTERNAL ? new_cell_from_split.fields.tableInternal.child_page : new_cell_from_split.fields.indexInternal.child_page;
                chidb_Btree_freeMemNode(bt, btn);
                return chidb_Btree_insertNonFull(bt, insertion_page, btc);
            }
        }
        chidb_Btree_freeMemNode(bt, btn);
        return chidb_Btree_insertNonFull(bt, insertion_page, btc);
    }
    else if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF)
    {
        if (j == btn->n_cells)
        {
            chilog(DEBUG, "Inserting cell into end of page %d at cell %d.", btn->page->npage, j);
            chidb_Btree_insertCell(btn, btn->n_cells, btc);
            int try_write = chidb_Btree_writeNode(bt, btn);
            chidb_Btree_freeMemNode(bt, btn);
            return try_write;
        }
        else if (j < btn->n_cells)
        {
            chidb_Btree_insertCell(btn, j, btc);
            int try_write = chidb_Btree_writeNode(bt, btn);
            chidb_Btree_freeMemNode(bt, btn);
            return try_write;
        }
    }
    else
    {
        return CHIDB_ECORRUPT;
    }
    chilog(CRITICAL, "Invalid node type %d!", btn->type);
    return CHIDB_ECORRUPT;
}

/* Split a B-Tree node
 *
 * Splits a B-Tree node N. This involves the following:
 * - Find the median cell in N.
 * - Create a new B-Tree node M.
 * - Move the cells before the median cell to M (if the
 *   cell is a table leaf cell, the median cell is moved too)
 * - Add a cell to the parent (which, by definition, will be an
 *   internal page) with the median key and the page number of M.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage_parent: Page number of the parent node
 * - npage_child: Page number of the node to split
 * - parent_ncell: Position in the parent where the new cell will
 *                 be inserted.
 * - npage_child2: Out parameter. Used to return the page of the new child node.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, ncell_t parent_ncell, npage_t *npage_child2)
{
    /* Your code goes here */
    BTreeNode *child_node;
    int try_get_child = chidb_Btree_getNodeByPage(bt, npage_child, &child_node);
    if (try_get_child != CHIDB_OK)
    {
        return try_get_child;
    }
    BTreeNode *parent_node;
    int try_get_parent = chidb_Btree_getNodeByPage(bt, npage_parent, &parent_node);
    if (try_get_parent != CHIDB_OK)
    {
        return try_get_parent;
    }
    npage_t new_page_n;
    int try_new_node = chidb_Btree_newNode(bt, &new_page_n, child_node->type);
    if (try_new_node != CHIDB_OK)
    {
        return try_new_node;
    }
    chilog(INFO, "SPLIT PAGE %d containing %d cells WITH PARENT %d, NEW PAGE IS %d",
           npage_child, child_node->n_cells, npage_parent, new_page_n);
    BTreeNode *new_node;
    int try_get_new_node = chidb_Btree_getNodeByPage(bt, new_page_n, &new_node);
    if (try_get_new_node != CHIDB_OK)
    {
        return try_get_new_node;
    }
    ncell_t median_cell_n = (child_node->n_cells - 1) / 2;
    BTreeCell median_cell;
    transfer_cells(bt, new_node, &child_node, median_cell_n + 1, &median_cell);
    BTreeCell insert_parent;
    insert_parent.type = parent_node->type;
    insert_parent.key = median_cell.key;
    if (parent_node->type == PGTYPE_TABLE_INTERNAL)
    {
        insert_parent.fields.tableInternal.child_page = new_page_n;
    }
    else if (parent_node->type == PGTYPE_INDEX_INTERNAL)
    {
        insert_parent.fields.indexInternal.child_page = new_page_n;
        insert_parent.fields.indexInternal.keyPk = median_cell.fields.indexInternal.keyPk;
    }
    chidb_Btree_insertCell(parent_node, parent_ncell, &insert_parent);
    *npage_child2 = new_page_n;

    chidb_Btree_writeNode(bt, parent_node);
    chidb_Btree_writeNode(bt, new_node);
    chidb_Btree_writeNode(bt, child_node);

    chidb_Btree_freeMemNode(bt, parent_node);
    chidb_Btree_freeMemNode(bt, new_node);
    chidb_Btree_freeMemNode(bt, child_node);

    return CHIDB_OK;
}
