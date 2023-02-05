/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors
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

#include "dbm-cursor.h"

int chidb_Cursor_open(chidb_dbm_cursor_t **cursor, chidb_dbm_cursor_type_t type, BTree *bt, npage_t npage, uint32_t col_n)
{
  chidb_dbm_cursor_t *_cursor = malloc(sizeof(chidb_dbm_cursor_t));
  _cursor->type = type;
  _cursor->bt = bt;
  _cursor->root_page_n = npage;
  _cursor->col_n = col_n;
  _cursor->nNodes = 1;
  _cursor->node_entries = malloc(sizeof(cursor_node_entry));
  chidb_Btree_getNodeByPage(bt, npage, &((_cursor->node_entries)[0].node));
  if ((_cursor->node_entries)[0].node->type == PGTYPE_INDEX_INTERNAL ||
      (_cursor->node_entries)[0].node->type == PGTYPE_INDEX_LEAF)
  {
    _cursor->tree_type = INDEX_CURSOR;
  }
  else if ((_cursor->node_entries)[0].node->type == PGTYPE_TABLE_INTERNAL ||
           (_cursor->node_entries)[0].node->type == PGTYPE_TABLE_LEAF)
  {
    _cursor->tree_type = TABLE_CURSOR;
  }
  chidb_Cursor_rewind(_cursor);
  *cursor = _cursor;
  return CHIDB_OK;
}

int chidb_Cursor_freeCursor(chidb_dbm_cursor_t *cursor)
{
  for (int i = 0; i < cursor->nNodes; i++)
  {
    chidb_Btree_freeMemNode(cursor->bt, cursor->node_entries[i].node);
  }
  return CHIDB_OK;
}

// set the ith entry of the node entries array in the cursor to have the node rooted at page npage, at the ncell entry.
int chidb_Cursor_setPathNode(chidb_dbm_cursor_t *cursor, npage_t npage, ncell_t ncell, uint32_t i)
{
  if (cursor->nNodes <= i)
  {
    realloc_nodes(cursor, i + 1);
  }
  BTreeNode *ptr;
  chidb_Btree_getNodeByPage(cursor->bt, npage, &ptr);
  cursor->node_entries[i].node = ptr;
  cursor->node_entries[i].ncell = ncell;
  if (ncell == ptr->n_cells)
  {
    return CHIDB_OK;
  }
  BTreeCell cell;
  chidb_Btree_getCell(ptr, ncell, &cell);
  cursor->node_entries[i].key = cell.key;
  return CHIDB_OK;
}

int realloc_nodes(chidb_dbm_cursor_t *cursor, uint32_t size)
{
  cursor->node_entries = realloc(cursor->node_entries, sizeof(cursor_node_entry) * size);
  cursor->nNodes = size;
  return CHIDB_OK;
}

int chidb_Cursor_rewindNode(chidb_dbm_cursor_t *cursor, npage_t npage, int index)
{
  BTreeNode *btn;
  BTreeCell curr_cell;
  chidb_Btree_getNodeByPage(cursor->bt, npage, &btn);
  chidb_Cursor_setPathNode(cursor, npage, 0, index);
  cursor->nNodes = index + 1;
  if (btn->n_cells == 0)
  {
    chilog(WARNING, "Cursor: Empty B Tree!");
    return CHIDB_CURSOR_EMPTY_BTREE;
  }
  chidb_Btree_getCell(btn, 0, &curr_cell);
  if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF)
  {
    cursor->curr_key = curr_cell.key;
    return CHIDB_OK;
  }
  else if (btn->type == PGTYPE_TABLE_INTERNAL)
  {
    return chidb_Cursor_rewindNode(cursor, curr_cell.fields.tableInternal.child_page, index + 1);
  }
  else if (btn->type == PGTYPE_INDEX_INTERNAL)
  {
    return chidb_Cursor_rewindNode(cursor, curr_cell.fields.indexInternal.child_page, index + 1);
  }
  return CHIDB_OK;
}

int chidb_Cursor_rewindNodeEnd(chidb_dbm_cursor_t *cursor, npage_t npage, int index)
{
  BTreeNode *btn;
  BTreeCell curr_cell;
  chidb_Btree_getNodeByPage(cursor->bt, npage, &btn);
  cursor->nNodes = index + 1;
  if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF)
  {
    chidb_Btree_getCell(btn, btn->n_cells - 1, &curr_cell);
    chidb_Cursor_setPathNode(cursor, npage, btn->n_cells - 1, index);
    cursor->curr_key = curr_cell.key;
    return CHIDB_OK;
  }
  else if (btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL)
  {
    chidb_Cursor_setPathNode(cursor, npage, btn->n_cells, index);
    return chidb_Cursor_rewindNodeEnd(cursor, btn->right_page, index + 1);
  }
  return CHIDB_OK;
}

int chidb_Cursor_rewind(chidb_dbm_cursor_t *cursor)
{
  chidb_Cursor_rewindNode(cursor, cursor->root_page_n, 0);
}

int chidb_Cursor_get(chidb_dbm_cursor_t *cursor, BTreeCell *cell)
{
  BTreeCell _cell;
  chidb_key_t key = cursor->curr_key;
  cursor_node_entry *entry = cursor->node_entries + (cursor->nNodes - 1);
  BTreeNode *btn = entry->node;
  for (int i = 0; i < btn->n_cells; i++)
  {
    chidb_Btree_getCell(btn, i, &_cell);
    if (_cell.key == cursor->curr_key)
    {
      break;
    }
  }
  *cell = _cell;
  return CHIDB_OK;
}

int chidb_Cursor_tableNextHelper(chidb_dbm_cursor_t *cursor, int cursor_node_n)
{
  cursor_node_entry *entry = cursor->node_entries + cursor_node_n;
  BTreeNode *btn = entry->node;
  BTreeCell cell;
  chilog(DEBUG, "%d cells, page %d, entry ncell %d", btn->n_cells, btn->page->npage, entry->ncell);
  if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF)
  {
    if (entry->ncell == btn->n_cells - 1 || btn->n_cells == 0 && cursor_node_n == 0)
    {
      if (cursor_node_n == 0)
      {
        return CHIDB_CURSOR_LAST_ENTRY;
      }
      return chidb_Cursor_tableNextHelper(cursor, cursor_node_n - 1);
    }
    else
    {
      entry->ncell += 1;
      chidb_Btree_getCell(btn, entry->ncell, &cell);
      entry->key = cell.key;
      cursor->curr_key = cell.key;
      return CHIDB_OK;
    }
  }
  else if (btn->type == PGTYPE_TABLE_INTERNAL)
  {
    if (entry->ncell == btn->n_cells)
    {
      if (cursor_node_n == 0)
      {
        return CHIDB_CURSOR_LAST_ENTRY;
      }
      return chidb_Cursor_tableNextHelper(cursor, cursor_node_n - 1);
    }
    else if (entry->ncell == btn->n_cells - 1)
    {
      entry->ncell += 1;
      return chidb_Cursor_rewindNode(cursor, btn->right_page, cursor_node_n + 1);
    }
    else
    {
      entry->ncell += 1;
      chidb_Btree_getCell(btn, entry->ncell, &cell);
      entry->key = cell.key;
      return chidb_Cursor_rewindNode(cursor, cell.fields.tableInternal.child_page, cursor_node_n + 1);
    }
  }
  else if (btn->type == PGTYPE_INDEX_INTERNAL)
  {
    if (entry->ncell == btn->n_cells)
    {
      if (cursor_node_n == 0)
      {
        return CHIDB_CURSOR_LAST_ENTRY;
      }
      return chidb_Cursor_tableNextHelper(cursor, cursor_node_n - 1);
    }
    else
    {
      if (entry->key > cursor->curr_key)
      {
        cursor->curr_key = entry->key;
        cursor->nNodes = cursor_node_n + 1;
        return CHIDB_OK;
      }
      else if (entry->key == cursor->curr_key)
      {
        entry->ncell += 1;
        if (entry->ncell == btn->n_cells)
        {
          return chidb_Cursor_rewindNode(cursor, btn->right_page, cursor_node_n + 1);
        }
        else
        {
          chidb_Btree_getCell(btn, entry->ncell, &cell);
          entry->key = cell.key;
          return chidb_Cursor_rewindNode(cursor, cell.fields.indexInternal.child_page, cursor_node_n + 1);
        }
      }
    }
  }
  return CHIDB_OK;
}

int chidb_Cursor_next(chidb_dbm_cursor_t *cursor)
{
  return chidb_Cursor_tableNextHelper(cursor, cursor->nNodes - 1);
}

int chidb_Cursor_tablePrevHelper(chidb_dbm_cursor_t *cursor, int cursor_node_n)
{
  cursor_node_entry *entry = cursor->node_entries + cursor_node_n;
  BTreeNode *btn = entry->node;
  BTreeCell cell;
  if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF)
  {
    if (entry->ncell == 0)
    {
      if (cursor_node_n == 0)
      {
        return CHIDB_CURSOR_FIRST_ENTRY;
      }
      return chidb_Cursor_tablePrevHelper(cursor, cursor_node_n - 1);
    }
    else
    {
      entry->ncell -= 1;
      chidb_Btree_getCell(btn, entry->ncell, &cell);
      entry->key = cell.key;
      cursor->curr_key = cell.key;
      return CHIDB_OK;
    }
  }
  else if (btn->type == PGTYPE_TABLE_INTERNAL)
  {
    if (entry->ncell == 0)
    {
      if (cursor_node_n == 0)
      {
        return CHIDB_CURSOR_FIRST_ENTRY;
      }
      return chidb_Cursor_tablePrevHelper(cursor, cursor_node_n - 1);
    }
    else if (entry->ncell > 0)
    {
      entry->ncell -= 1;
      chidb_Btree_getCell(btn, entry->ncell, &cell);
      entry->key = cell.key;
      return chidb_Cursor_rewindNodeEnd(cursor, cell.fields.tableInternal.child_page, cursor_node_n + 1);
    }
  }
  else if (btn->type == PGTYPE_INDEX_INTERNAL)
  {
    if (entry->ncell == 0)
    {
      if (cursor_node_n == 0)
      {
        return CHIDB_CURSOR_FIRST_ENTRY;
      }
      return chidb_Cursor_tablePrevHelper(cursor, cursor_node_n - 1);
    }
    else if (entry->ncell == btn->n_cells)
    {
      entry->ncell = btn->n_cells - 1;
      chidb_Btree_getCell(btn, btn->n_cells - 1, &cell);
      entry->key = cell.key;
      cursor->curr_key = cell.key;
      cursor->nNodes = cursor_node_n + 1;
      return CHIDB_OK;
    }
    else
    {
      if (entry->key == cursor->curr_key)
      {
        chidb_Btree_getCell(btn, entry->ncell, &cell);
        return chidb_Cursor_rewindNodeEnd(cursor, cell.fields.indexInternal.child_page, cursor_node_n + 1);
      }
      else
      {
        entry->ncell -= 1;
        chidb_Btree_getCell(btn, entry->ncell, &cell);
        entry->key = cell.key;
        cursor->curr_key = cell.key;
        cursor->nNodes = cursor_node_n + 1;
      }
    }
  }
  return CHIDB_OK;
}

int chidb_Cursor_prev(chidb_dbm_cursor_t *cursor)
{
  return chidb_Cursor_tablePrevHelper(cursor, cursor->nNodes - 1);
}

// this function assumes that the key exists somewhere in the tree rooted at
// the indexth entry of the node entries of the cursor.
int chidb_Cursor_setKey(chidb_dbm_cursor_t *cursor, chidb_key_t key, int index)
{
  cursor_node_entry *entry = cursor->node_entries + index;
  BTreeNode *btn = entry->node;
  if (btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL)
  {
    for (int i = 0; i < btn->n_cells; i++)
    {
      BTreeCell curr_cell;
      chidb_Btree_getCell(btn, i, &curr_cell);
      if (key <= curr_cell.key)
      {
        entry->ncell = i;
        entry->key = curr_cell.key;
        if (btn->type == PGTYPE_TABLE_INTERNAL)
        {
          chidb_Cursor_setPathNode(cursor, curr_cell.fields.tableInternal.child_page, 0, index + 1);
        }
        else if (btn->type == PGTYPE_INDEX_INTERNAL)
        {
          if (key == curr_cell.key)
          {
            cursor->curr_key = key;
            cursor->nNodes = index + 1;
            return CHIDB_OK;
          }
          chidb_Cursor_setPathNode(cursor, curr_cell.fields.indexInternal.child_page, 0, index + 1);
        }
        return chidb_Cursor_setKey(cursor, key, index + 1);
      }
    }
    entry->ncell = btn->n_cells;
    chidb_Cursor_setPathNode(cursor, btn->right_page, 0, index + 1);
    return chidb_Cursor_setKey(cursor, key, index + 1);
  }
  else if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF)
  {
    for (int i = 0; i < btn->n_cells; i++)
    {
      BTreeCell curr_cell;
      chidb_Btree_getCell(btn, i, &curr_cell);
      if (key == curr_cell.key)
      {
        entry->key = key;
        entry->ncell = i;
        cursor->curr_key = key;
        cursor->nNodes = index + 1;
        return CHIDB_OK;
      }
    }
  }
}

int chidb_Cursor_seekHelper(chidb_dbm_cursor_t *cursor, chidb_key_t key, int index)
{
  cursor_node_entry *entry = cursor->node_entries + index;
  uint8_t *data;
  uint8_t size;
  int try_find = chidb_Btree_find(cursor->bt, entry->node->page->npage, key, &data, &size);
  if (try_find == CHIDB_ENOTFOUND)
  {
    if (index == 0)
    {
      return CHIDB_ENOTFOUND;
    }
    else
    {
      return chidb_Cursor_seekHelper(cursor, key, index - 1);
    }
  }
  else if (try_find == CHIDB_OK)
  {
    chidb_Cursor_setKey(cursor, key, index);
    return CHIDB_OK;
  }
  return CHIDB_OK;
}

int chidb_Cursor_seek(chidb_dbm_cursor_t *cursor, chidb_key_t key)
{
  return chidb_Cursor_seekHelper(cursor, key, cursor->nNodes - 1);
}

// Go to the position in the btree that key would be at, regardless of whether
// it is actually in the btree. If the key exists in the btree, then the position
// will be at the key. If the key doesn't exist in the btree, then the position will be
// one of two possibilities: first, there exists a key greater than the given key in the
// leaf node that is navigated to. In this case, the cursor is at the insertion position
// of the key if it were to be inserted into the btree. Second is that the given key is
// greater than all of the keys in the leaf node. Then the cursor will be set to the last
// cell of the leaf node.
int chidb_Cursor_goToPositionHelper(chidb_dbm_cursor_t *cursor, chidb_key_t key, int index)
{
  cursor_node_entry *entry = cursor->node_entries + index;
  BTreeNode *btn = entry->node;
  if (btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL)
  {
    for (int i = 0; i < btn->n_cells; i++)
    {
      BTreeCell curr_cell;
      chidb_Btree_getCell(btn, i, &curr_cell);
      if (key <= curr_cell.key)
      {
        entry->ncell = i;
        entry->key = curr_cell.key;
        if (btn->type == PGTYPE_TABLE_INTERNAL)
        {
          chidb_Cursor_setPathNode(cursor, curr_cell.fields.tableInternal.child_page, 0, index + 1);
        }
        else if (btn->type == PGTYPE_INDEX_INTERNAL)
        {
          if (key == curr_cell.key)
          {
            cursor->curr_key = key;
            cursor->nNodes = index + 1;
            return CHIDB_OK;
          }
          chidb_Cursor_setPathNode(cursor, curr_cell.fields.indexInternal.child_page, 0, index + 1);
        }
        return chidb_Cursor_goToPositionHelper(cursor, key, index + 1);
      }
    }
    entry->ncell = btn->n_cells;
    chidb_Cursor_setPathNode(cursor, btn->right_page, 0, index + 1);
    return chidb_Cursor_goToPositionHelper(cursor, key, index + 1);
  }
  else if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF)
  {
    for (int i = 0; i < btn->n_cells; i++)
    {
      BTreeCell curr_cell;
      chidb_Btree_getCell(btn, i, &curr_cell);
      if (key <= curr_cell.key || i == btn->n_cells - 1)
      {
        entry->key = curr_cell.key;
        entry->ncell = i;
        cursor->curr_key = curr_cell.key;
        cursor->nNodes = index + 1;
        return CHIDB_OK;
      }
    }
  }
}

int chidb_Cursor_goToPosition(chidb_dbm_cursor_t *cursor, chidb_key_t key)
{
  return chidb_Cursor_goToPositionHelper(cursor, key, 0);
}

int chidb_Cursor_seekGt(chidb_dbm_cursor_t *cursor, chidb_key_t key)
{
  chidb_Cursor_goToPosition(cursor, key);
  if (key >= cursor->curr_key)
  {
    int try_next = chidb_Cursor_next(cursor);
    return try_next;
  }
  else
  {
    return CHIDB_OK;
  }
}

int chidb_Cursor_seekGte(chidb_dbm_cursor_t *cursor, chidb_key_t key)
{
  chidb_Cursor_goToPosition(cursor, key);
  if (key > cursor->curr_key)
  {
    int try_next = chidb_Cursor_next(cursor);
    return try_next;
  }
  else
  {
    return CHIDB_OK;
  }
}

int chidb_Cursor_seekLt(chidb_dbm_cursor_t *cursor, chidb_key_t key)
{
  chidb_Cursor_goToPosition(cursor, key);
  if (key <= cursor->curr_key)
  {
    int try_prev = chidb_Cursor_prev(cursor);
    return try_prev;
  }
  else
  {
    return CHIDB_OK;
  }
}

int chidb_Cursor_seekLte(chidb_dbm_cursor_t *cursor, chidb_key_t key)
{
  chidb_Cursor_goToPosition(cursor, key);
  if (key < cursor->curr_key)
  {
    int try_prev = chidb_Cursor_prev(cursor);
    return try_prev;
  }
  else
  {
    return CHIDB_OK;
  }
}
