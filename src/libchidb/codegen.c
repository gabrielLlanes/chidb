/*
 *  chidb - a didactic relational database management system
 *
 *  SQL -> DBM Code Generator
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

#include <chidb/chidb.h>
#include <chisql/chisql.h>
#include "dbm.h"
#include "util.h"

/* ...code... */

static int check_cols_exist_expr(chidb_stmt *stmt, char *table_name, Expression_t *cols, int nCols);

static int check_cols_exist_strlist(chidb_stmt *stmt, char *table_name, StrList_t *cols, int nCols);

static int chidb_stmt_codegen_simple_select(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols, int pkey_n, int root_npage);

static int chidb_stmt_codegen_simple_select_where(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols, int pkey_n, int root_npage);

static int chidb_stmt_codegen_simple_insert(chidb_stmt *stmt, chisql_statement_t *sql_stmt);

static int chidb_stmt_codegen_simple_create_table(chidb_stmt *stmt, chisql_statement_t *sql_stmt);

static int chidb_stmt_codegen_create_index(chidb_stmt *stmt, chisql_statement_t *sql_stmt);

static int chidb_stmt_validate_schema_exists(chidb_stmt *stmt, char *schema_name, int *root_npage)
{
  npage_t _root_npage = schema_root_page(stmt->db, schema_name);
  if (_root_npage == 0)
  {
    chilog(CRITICAL, "INVALID DNE");
    return CHIDB_EINVALIDSQL;
  }
  *root_npage = _root_npage;
  return CHIDB_OK;
}

static int chidb_stmt_validate_project_all_cols(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int *nCols, int *pkey_n)
{
  SRA_Project_t sra_project = sql_stmt->stmt.select->project;
  SRA_Table_t sra_table;
  if (sra_project.sra->t == SRA_SELECT)
  {
    sra_table = sra_project.sra->select.sra->table;
  }
  else
  {
    sra_table = sra_project.sra->table;
  }
  *nCols = table_ncols(stmt->db, sra_table.ref->table_name);
  stmt->cols = malloc(*nCols * sizeof(char *));
  ChidbSchema schema;
  get_schema(stmt->db, sra_table.ref->table_name, &schema);
  Column_t *col = schema.table->columns;
  for (int i = 0; i < *nCols; i++, col = col->next)
  {
    if (is_pkey(stmt->db, sra_table.ref->table_name, col->name))
    {
      *pkey_n = i;
    }
    stmt->cols[i] = col->name;
  }
  return CHIDB_OK;
}

static int chidb_stmt_validate_project_selected_cols(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int *nCols, int *pkey_n)
{
  SRA_Project_t sra_project = sql_stmt->stmt.select->project;
  SRA_Table_t sra_table;
  if (sra_project.sra->t == SRA_SELECT)
  {
    sra_table = sra_project.sra->select.sra->table;
  }
  else
  {
    sra_table = sra_project.sra->table;
  }
  Expression_t *cols = sra_project.expr_list;
  int _nCols = 0;
  Expression_t *curr_col = cols;
  while (curr_col != NULL)
  {
    _nCols++;
    curr_col = curr_col->next;
  }
  *nCols = _nCols;
  int cols_exist = check_cols_exist_expr(stmt, sra_table.ref->table_name, cols, _nCols);
  if (!cols_exist)
  {
    return CHIDB_EINVALIDSQL;
  }
  curr_col = cols;
  *pkey_n = -1;
  stmt->cols = malloc(_nCols * sizeof(char *));
  for (int i = 0; curr_col != NULL && i < _nCols; i++, curr_col = curr_col->next)
  {
    if (is_pkey(stmt->db, sra_table.ref->table_name, curr_col->expr.term.ref->columnName))
    {
      *pkey_n = i;
    }
    stmt->cols[i] = curr_col->expr.term.ref->columnName;
  }
  return CHIDB_OK;
}

static int chidb_stmt_validate_project_cols(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int *nCols, int *pkey_n)
{
  SRA_Project_t sra_project = sql_stmt->stmt.select->project;
  Expression_t *cols = sra_project.expr_list;
  if (strcmp(cols->expr.term.ref->columnName, "*") == 0)
  {
    return chidb_stmt_validate_project_all_cols(stmt, sql_stmt, nCols, pkey_n);
  }
  else
  {
    return chidb_stmt_validate_project_selected_cols(stmt, sql_stmt, nCols, pkey_n);
  }
}

static int chidb_stmt_validate_insert_cols(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int *nCols, int *pkey_n)
{
  Insert_t *insert = sql_stmt->stmt.insert;
  StrList_t *col_names = insert->col_names;
  int _nCols = 0;
  while (col_names != NULL)
  {
    _nCols++;
    col_names = col_names->next;
  }
  *nCols = _nCols;
  col_names = insert->col_names;
  if (!check_cols_exist_strlist(stmt, insert->table_name, col_names, _nCols))
  {
    return CHIDB_EINVALIDSQL;
  }
  *pkey_n = -1;
  for (int i = 0; col_names != NULL && i < _nCols; i++, col_names = col_names->next)
  {
    if (is_pkey(stmt->db, insert->table_name, col_names->str))
    {
      *pkey_n = i;
      chilog(DEBUG, "Found pkey for insert at %d", *pkey_n);
    }
  }
  return CHIDB_OK;
}

static int chidb_stmt_validate_simple_select(chidb_stmt *stmt, chisql_statement_t *sql_stmt)
{
  SRA_t *select = sql_stmt->stmt.select;
  SRA_Project_t sra_project = select->project;
  SRA_Select_t sra_select = sra_project.sra->select;
  SRA_Table_t sra_table = sra_select.sra->table;
  char *cmp_col_name = sra_select.cond->cond.comp.expr1->expr.term.ref->columnName;
  enum data_type col_type = table_col_type(stmt->db, sra_table.ref->table_name, cmp_col_name);
  enum data_type cmp_type = sra_select.cond->cond.comp.expr2->expr.term.val->t;
  if (col_type != cmp_type)
  {
    // support comparison of a string to a char
    if (cmp_type == TYPE_CHAR || col_type == TYPE_TEXT)
    {
      select->project.sra->select.cond->cond.comp.expr2->expr.term.val->t = TYPE_TEXT;
      char *str_from_char = malloc(2);
      str_from_char[0] = select->project.sra->select.cond->cond.comp.expr2->expr.term.val->val.cval;
      str_from_char[1] = '\0';
      select->project.sra->select.cond->cond.comp.expr2->expr.term.val->val.strval = str_from_char;
    }
    else
    {
      chilog(CRITICAL, "Type mismatch!, col type %d vs literal type %d", col_type, cmp_type);
      return CHIDB_EINVALIDSQL;
    }
  }
  return CHIDB_OK;
}

int chidb_stmt_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt)
{
  load_schema(stmt->db);
  if (stmt->ops == NULL)
  {
    chilog(DEBUG, "null ops: initalizing.");
    stmt->ops = malloc(sizeof(chidb_dbm_op_t));
  }
  if (sql_stmt->type == STMT_SELECT && sql_stmt->stmt.select->t == SRA_PROJECT &&
      sql_stmt->stmt.select->project.sra->t == SRA_SELECT &&
      sql_stmt->stmt.select->project.sra->select.sra->t == SRA_TABLE)
  {
    int root_npage;
    int pkey_n;
    int nCols;
    SRA_Project_t sra_project = sql_stmt->stmt.select->project;
    SRA_Select_t sra_select = sra_project.sra->select;
    SRA_Table_t sra_table = sra_select.sra->table;
    chilog(DEBUG, "Validating schema %s exists", sra_table.ref->table_name);
    if (chidb_stmt_validate_schema_exists(stmt, sra_table.ref->table_name, &root_npage) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    chilog(DEBUG, "Validating projection cols");
    if (chidb_stmt_validate_project_cols(stmt, sql_stmt, &nCols, &pkey_n) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    chilog(DEBUG, "Validating simple select clause");
    if (chidb_stmt_validate_simple_select(stmt, sql_stmt) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    return chidb_stmt_codegen_simple_select_where(stmt, sql_stmt, nCols, pkey_n, root_npage);
  }
  else if (sql_stmt->type == STMT_SELECT && sql_stmt->stmt.select->t == SRA_PROJECT &&
           sql_stmt->stmt.select->project.sra->t == SRA_TABLE)
  {
    int root_npage;
    int pkey_n;
    int nCols;
    SRA_Project_t sra_project = sql_stmt->stmt.select->project;
    SRA_Table_t sra_table = sra_project.sra->table;
    if (chidb_stmt_validate_schema_exists(stmt, sra_table.ref->table_name, &root_npage) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    if (chidb_stmt_validate_project_cols(stmt, sql_stmt, &nCols, &pkey_n) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    return chidb_stmt_codegen_simple_select(stmt, sql_stmt, nCols, pkey_n, root_npage);
  }
  else if (sql_stmt->type == STMT_INSERT)
  {
    return chidb_stmt_codegen_simple_insert(stmt, sql_stmt);
  }
  else if (sql_stmt->type == STMT_CREATE && sql_stmt->stmt.create->t == CREATE_TABLE)
  {
    return chidb_stmt_codegen_simple_create_table(stmt, sql_stmt);
  }
  else if (sql_stmt->type == STMT_CREATE && sql_stmt->stmt.create->t == CREATE_INDEX)
  {
    chilog(DEBUG, "Creating index");
    return chidb_stmt_codegen_create_index(stmt, sql_stmt);
  }
  int opnum = 0;
  int nOps;

  /* Manually load a program that just produces five result rows, with
   * three columns: an integer identifier, the SQL query (text), and NULL. */

  stmt->nCols = 3;
  stmt->cols = malloc(sizeof(char *) * stmt->nCols);
  stmt->cols[0] = strdup("id");
  stmt->cols[1] = strdup("sql");
  stmt->cols[2] = strdup("null");

  chidb_dbm_op_t ops[] = {
      {Op_Integer, 1, 0, 0, NULL},
      {Op_String, strlen(sql_stmt->text), 1, 0, sql_stmt->text},
      {Op_Null, 0, 2, 0, NULL},
      {Op_ResultRow, 0, 3, 0, NULL},
      {Op_Integer, 2, 0, 0, NULL},
      {Op_ResultRow, 0, 3, 0, NULL},
      {Op_Integer, 3, 0, 0, NULL},
      {Op_ResultRow, 0, 3, 0, NULL},
      {Op_Integer, 4, 0, 0, NULL},
      {Op_ResultRow, 0, 3, 0, NULL},
      {Op_Integer, 5, 0, 0, NULL},
      {Op_ResultRow, 0, 3, 0, NULL},
      {Op_Halt, 0, 0, 0, NULL},
  };

  nOps = sizeof(ops) / sizeof(chidb_dbm_op_t);

  for (int i = 0; i < nOps; i++)
    chidb_stmt_set_op(stmt, &ops[i], opnum++);

  return CHIDB_OK;
}

static int check_cols_exist_expr(chidb_stmt *stmt, char *table_name, Expression_t *cols, int nCols)
{
  Expression_t *curr_col = cols;
  while (curr_col != NULL)
  {
    // nCols++;
    if (table_col_exists(stmt->db, table_name, curr_col->expr.term.ref->columnName) == 0)
    {
      return 0;
    }
    curr_col = curr_col->next;
  }
  return 1;
}

static int check_cols_exist_strlist(chidb_stmt *stmt, char *table_name, StrList_t *cols, int nCols)
{
  StrList_t *curr_col = cols;
  while (curr_col != NULL)
  {
    if (table_col_exists(stmt->db, table_name, curr_col->str) == 0)
    {
      return 0;
    }
    curr_col = curr_col->next;
  }
  return 1;
}

// fills instructions addr_start thru addr_start + nCols + 2.
static int simple_col_codegen(chidb_stmt *stmt, int addr_start, int cursor, int *cols, int nCols, int next_jump_addr, int base_reg, int pkey_n)
{
  for (int i = 0; i < nCols; i++)
  {
    if (i == pkey_n)
    {
      chidb_dbm_op_t op = {Op_Key, cursor, base_reg + i, 0, NULL};
      chidb_stmt_set_op(stmt, &op, addr_start + i);
    }
    else
    {
      chidb_dbm_op_t op = {Op_Column, cursor, cols[i], base_reg + i, NULL};
      chidb_stmt_set_op(stmt, &op, addr_start + i);
    }
  }
  // now set the result row instruction
  chidb_dbm_op_t op_resultRow = {Op_ResultRow, base_reg, nCols, 0, NULL};
  chidb_stmt_set_op(stmt, &op_resultRow, addr_start + nCols);
  // then set the next instruction
  chidb_dbm_op_t op_next = {Op_Next, cursor, next_jump_addr, 0, NULL};
  chidb_stmt_set_op(stmt, &op_next, addr_start + nCols + 1);
  return CHIDB_OK;
}

static int chidb_stmt_codegen_simple_select(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols, int pkey_n, int root_npage)
{
  SRA_t *select = sql_stmt->stmt.select;
  SRA_Project_t sra_project = select->project;
  SRA_Table_t sra_table = sra_project.sra->table;
  ChidbSchema schema;
  get_schema(stmt->db, sra_table.ref->table_name, &schema);
  Expression_t *cols = sra_project.expr_list;
  if (strcmp(cols->expr.term.ref->columnName, "*") == 0)
  {
    chilog(DEBUG, "Select * detected.");
    int cols_a[nCols];
    for (int i = 0; i < nCols; i++)
    {
      cols_a[i] = i;
    }
    simple_col_codegen(stmt, 3, 0, cols_a, nCols, 3, 1, pkey_n);
  }
  else
  {
    chilog(DEBUG, "Query specified cols");
    Expression_t *curr_col = cols;
    int cols_a[nCols];
    for (int i = 0; curr_col != NULL && i < nCols; i++, curr_col = curr_col->next)
    {
      cols_a[i] = table_col_exists(stmt->db, sra_table.ref->table_name, curr_col->expr.term.ref->columnName) - 1;
    }
    simple_col_codegen(stmt, 3, 0, cols_a, nCols, 3, 1, pkey_n);
  }
  stmt->nCols = nCols;
  stmt->nRR = nCols;
  chilog(DEBUG, "%d cols", nCols);
  chidb_dbm_op_t op_int = {Op_Integer, root_npage, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_int, 0);
  chidb_dbm_op_t op_openRead = {Op_OpenRead, 0, 0, table_ncols(stmt->db, sra_table.ref->table_name), NULL};
  chidb_stmt_set_op(stmt, &op_openRead, 1);
  chidb_dbm_op_t op_rewind = {Op_Rewind, 0, 3 + nCols + 2, 0, NULL};
  chidb_stmt_set_op(stmt, &op_rewind, 2);
  chidb_dbm_op_t op_close = {Op_Close, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_close, 3 + nCols + 2);
  chidb_dbm_op_t op_halt = {Op_Halt, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_halt, 3 + nCols + 3);
  stmt->pc = 0;
  chilog(DEBUG, "Done generating code for %s", sql_stmt->text);
  return CHIDB_OK;
}

static enum opcode simple_cmp_condtype_opcode(enum CondType condtype)
{
  if (condtype == RA_COND_EQ)
  {
    return Op_Eq;
  }
  else if (condtype == RA_COND_GEQ)
  {
    return Op_Ge;
  }
  else if (condtype == RA_COND_GT)
  {
    return Op_Gt;
  }
  else if (condtype == RA_COND_LEQ)
  {
    return Op_Le;
  }
  else
  {
    return Op_Lt;
  }
}

static int chidb_stmt_codegen_range_query_indexed(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols)
{
}

static int chidb_stmt_codegen_simple_select_where(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols, int pkey_n, int root_npage)
{
  SRA_t *select = sql_stmt->stmt.select;
  SRA_Project_t sra_project = select->project;
  SRA_Select_t sra_select = sra_project.sra->select;
  SRA_Table_t sra_table = sra_select.sra->table;
  // check the select where clause
  char *cmp_col_name = sra_select.cond->cond.comp.expr1->expr.term.ref->columnName;
  enum data_type cmp_type = sra_select.cond->cond.comp.expr2->expr.term.val->t;
  if (is_pkey(stmt->db, sra_table.ref->table_name, cmp_col_name))
  {
    chidb_dbm_op_t op_key = {Op_Key, 0, 2, 0, NULL};
    chidb_stmt_set_op(stmt, &op_key, 4);
  }
  else
  {
    chidb_dbm_op_t op_column = {Op_Column, 0, table_col_n(stmt->db, sra_table.ref->table_name, cmp_col_name), 2, NULL};
    chidb_stmt_set_op(stmt, &op_column, 4);
  }

  if (cmp_type == TYPE_INT)
  {
    chidb_dbm_op_t op_int = {Op_Integer, sra_select.cond->cond.comp.expr2->expr.term.val->val.ival, 1, 0, NULL};
    chidb_stmt_set_op(stmt, &op_int, 3);
  }
  else if (cmp_type == TYPE_TEXT)
  {
    char *strval = sra_select.cond->cond.comp.expr2->expr.term.val->val.strval;
    chidb_dbm_op_t op_text = {Op_String, strlen(strval), 1, 0, strval};
    chidb_stmt_set_op(stmt, &op_text, 3);
  }
  chidb_dbm_op_t op_int = {Op_Integer, root_npage, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_int, 0);
  ChidbSchema schema;
  get_schema(stmt->db, sra_table.ref->table_name, &schema);
  Expression_t *cols = sra_project.expr_list;
  if (strcmp(cols->expr.term.ref->columnName, "*") == 0)
  {
    chilog(DEBUG, "Select * detected");
    Column_t *col = schema.table->columns;
    int cols_a[nCols];
    for (int i = 0; i < nCols; i++, col = col->next)
    {
      cols_a[i] = i;
    }
    simple_col_codegen(stmt, 7, 0, cols_a, nCols, 4, 3, pkey_n);
  }
  else
  {
    chilog(DEBUG, "Query specified cols");
    Expression_t *curr_col = cols;
    int cols_a[nCols];
    for (int i = 0; curr_col != NULL && i < nCols; i++, curr_col = curr_col->next)
    {
      cols_a[i] = table_col_exists(stmt->db, sra_table.ref->table_name, curr_col->expr.term.ref->columnName) - 1;
    }
    simple_col_codegen(stmt, 7, 0, cols_a, nCols, 4, 3, pkey_n);
  }
  stmt->nCols = nCols;
  stmt->nRR = nCols;
  chilog(DEBUG, "%d cols", nCols);
  chidb_dbm_op_t op_cmp = {simple_cmp_condtype_opcode(sra_select.cond->t), 1, 7, 2, NULL};
  chidb_stmt_set_op(stmt, &op_cmp, 5);
  chidb_dbm_op_t op_unconditional_jump = {Op_Eq, 1, 7 + nCols + 1, 1, NULL};
  chidb_stmt_set_op(stmt, &op_unconditional_jump, 6);
  chidb_dbm_op_t op_openRead = {Op_OpenRead, 0, 0, table_ncols(stmt->db, sra_table.ref->table_name), NULL};
  chidb_stmt_set_op(stmt, &op_openRead, 1);
  chidb_dbm_op_t op_rewind = {Op_Rewind, 0, 7 + nCols + 2, 0, NULL};
  chidb_stmt_set_op(stmt, &op_rewind, 2);
  chidb_dbm_op_t op_close = {Op_Close, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_close, 7 + nCols + 2);
  chidb_dbm_op_t op_halt = {Op_Halt, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_halt, 7 + nCols + 3);
  stmt->pc = 0;
  return CHIDB_OK;
}

static int insert_set_all_cols(Insert_t *insert, ChidbSchema *schema)
{
  chilog(DEBUG, "Null cols, modifying as all.");
  int nCols = 0;
  Column_t *cols = schema->table->columns;
  while (cols != NULL)
  {
    nCols++;
    cols = cols->next;
  }
  cols = schema->table->columns;
  insert->col_names = malloc(sizeof(StrList_t));
  StrList_t *insert_cols = insert->col_names;
  for (int i = 0; i < nCols; i++)
  {
    char *s = malloc(strlen(cols->name));
    strcpy(s, cols->name);
    insert_cols->str = s;
    cols = cols->next;
    if (i < nCols - 1)
    {
      insert_cols->next = malloc(sizeof(StrList_t));
      insert_cols = insert_cols->next;
    }
    else
    {
      insert_cols->next = NULL;
    }
  }
  insert_cols = insert->col_names;
  while (insert_cols != NULL)
  {
    insert_cols = insert_cols->next;
  }
  return CHIDB_OK;
}

static int verify_insert_value_types(enum data_type *types, int nCols, Literal_t *values, int *nValues)
{
  int _nValues = 0;
  Literal_t *curr_val = values;
  while (curr_val != NULL)
  {
    chilog(DEBUG, "%d %d type compare", curr_val->t, types[_nValues % nCols]);
    if (curr_val->t != types[_nValues % nCols])
    {
      return CHIDB_EINVALIDSQL;
    }
    _nValues++;
    curr_val = curr_val->next;
  }
  if (_nValues % nCols != 0)
  {
    return CHIDB_EINVALIDSQL;
  }
  chilog(DEBUG, "Verified values. %d values", _nValues);
  *nValues = _nValues;
  return CHIDB_OK;
}

static int simple_insert_codegen_record(chidb_stmt *stmt, Literal_t *values, int addr_start, int nCols, int base_reg, int pkey_n)
{
  Literal_t *curr_val = values;
  for (int i = 0, j = 0; i < nCols; i++, curr_val = curr_val->next)
  {
    if (i == pkey_n)
    {
      chilog(DEBUG, "Address %d: (INTEGER %d %d %d %s) for primary key",
             addr_start + j, curr_val->val.ival, base_reg + nCols, 0, NULL);
      chidb_dbm_op_t op_keyInt = {Op_Integer, curr_val->val.ival, base_reg + nCols, 0, NULL};
      chidb_stmt_set_op(stmt, &op_keyInt, addr_start + j);
      j++;
      chilog(DEBUG, "Address %d: (NULL %d %d %d %s) for primary key",
             addr_start + j, 0, base_reg + i, 0, NULL);
      chidb_dbm_op_t op_null = {Op_Null, 0, base_reg + i, 0, NULL};
      chidb_stmt_set_op(stmt, &op_null, addr_start + j);
      j++;
    }
    else
    {
      if (curr_val->t == TYPE_TEXT)
      {
        chilog(DEBUG, "Address %d: (STRING %d %d %d %s)",
               addr_start + j, strlen(curr_val->val.strval), base_reg + i, 0, curr_val->val.strval);
        chidb_dbm_op_t op_string = {Op_String, strlen(curr_val->val.strval), base_reg + i, 0, curr_val->val.strval};
        chidb_stmt_set_op(stmt, &op_string, addr_start + j);
      }
      else if (curr_val->t == TYPE_INT)
      {
        chilog(DEBUG, "Address %d: (INTEGER %d %d %d %s)",
               addr_start + j, curr_val->val.ival, base_reg + i, 0, NULL);
        chidb_dbm_op_t op_int = {Op_Integer, curr_val->val.ival, base_reg + i, 0, NULL};
        chidb_stmt_set_op(stmt, &op_int, addr_start + j);
      }
      j++;
    }
  }
  chidb_dbm_op_t op_record = {Op_MakeRecord, base_reg, nCols, base_reg + nCols + 1, NULL};
  chilog(DEBUG, "Address %d: (MAKE_RECORD, %d, %d, %d, %s)",
         addr_start + nCols + 1, base_reg, nCols, base_reg + nCols + 1, NULL);
  chidb_stmt_set_op(stmt, &op_record, addr_start + nCols + 1);
  chidb_dbm_op_t op_insert = {Op_Insert, 0, base_reg + nCols + 1, base_reg + nCols, NULL};
  chilog(DEBUG, "Address %d: (INSERT, %d, %d, %d, %s)",
         addr_start + nCols + 2, 0, base_reg + nCols + 1, base_reg + nCols, NULL);
  chidb_stmt_set_op(stmt, &op_insert, addr_start + nCols + 2);
  return CHIDB_OK;
}

static int simple_insert_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int addr_start, int nCols, int nValues, int base_reg, int pkey_n)
{
  Insert_t *insert = sql_stmt->stmt.insert;
  Literal_t *values = insert->values;
  Literal_t *curr_values = values;
  int nRecords = nValues / nCols;
  for (int i = 0, curr_addr_start = addr_start; i < nRecords; i++, curr_addr_start += (nCols + 3))
  {
    chilog(DEBUG, "Generating code for record %d / %d, pkey at column %d", i + 1, nRecords, pkey_n);
    simple_insert_codegen_record(stmt, curr_values, curr_addr_start, nCols, base_reg, pkey_n);
    for (int j = 0; j < nCols; j++)
    {
      curr_values = curr_values->next;
    }
  }
  return CHIDB_OK;
}

static int chidb_stmt_codegen_simple_insert(chidb_stmt *stmt, chisql_statement_t *sql_stmt)
{
  chilog(DEBUG, "Code gen for simple insert.");
  Insert_t *insert = sql_stmt->stmt.insert;
  ChidbSchema schema;
  get_schema(stmt->db, insert->table_name, &schema);
  int root_npage;
  if (chidb_stmt_validate_schema_exists(stmt, insert->table_name, &root_npage) != CHIDB_OK)
  {
    return CHIDB_EINVALIDSQL;
  }
  if (insert->col_names == NULL)
  {
    insert_set_all_cols(insert, &schema);
  }
  int nCols;
  int pkey_n;
  int nValues;
  if (chidb_stmt_validate_insert_cols(stmt, sql_stmt, &nCols, &pkey_n) != CHIDB_OK)
  {
    return CHIDB_EINVALIDSQL;
  }
  chilog(DEBUG, "Pkey %d, ncols %d, root %d", pkey_n, nCols, root_npage);
  StrList_t *col_names = insert->col_names;
  Literal_t *values = insert->values;
  enum data_type types[nCols];
  for (int i = 0; i < nCols; i++, col_names = col_names->next)
  {
    types[i] = table_col_type(stmt->db, insert->table_name, col_names->str);
  }
  if (verify_insert_value_types(types, nCols, values, &nValues) != CHIDB_OK)
  {
    chilog(WARNING, "INVALIDATED VALUES");
    return CHIDB_EINVALIDSQL;
  }
  col_names = insert->col_names;
  chidb_dbm_op_t op_int = {Op_Integer, root_npage, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_int, 0);
  chidb_dbm_op_t op_openwrite = {Op_OpenWrite, 0, 0, table_ncols(stmt->db, insert->table_name), NULL};
  chidb_stmt_set_op(stmt, &op_openwrite, 1);
  chidb_dbm_op_t op_rewind = {Op_Rewind, 0, 3, 0, NULL};
  chidb_stmt_set_op(stmt, &op_rewind, 2);
  simple_insert_codegen(stmt, sql_stmt, 3, nCols, nValues, 1, pkey_n);
  int nRecords = nValues / nCols;
  int end_addr = 3 + ((nCols + 3) * nRecords);
  chidb_dbm_op_t op_close = {Op_Close, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_close, end_addr);
  chilog(DEBUG, "Setting close / halt in addr %d", end_addr);
  chidb_dbm_op_t op_halt = {Op_Halt, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_halt, end_addr + 1);
  stmt->pc = 0;
  return CHIDB_OK;
}

static int chidb_stmt_validate_simple_create_table(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int *nCols)
{
  Create_t *create = sql_stmt->stmt.create;
  Column_t *cols = create->table->columns;
  Column_t *col = cols;
  if (schema_exists(stmt->db, create->table->name))
  {
    chilog(CRITICAL, "Table %s exists already! Aborting.", create->table->name);
    return CHIDB_EINVALIDSQL;
  }
  int _nCols = 0;
  while (col != NULL)
  {
    if (_nCols == 0)
    {
      if (!(col->type == TYPE_INT && col->constraints->t == CONS_PRIMARY_KEY))
      {
        chilog(CRITICAL, "First column %s needs to be integer and primary key!", col->name);
        return CHIDB_EINVALIDSQL;
      }
    }
    else
    {
      if (!(col->constraints == NULL && (col->type == TYPE_INT || col->type == TYPE_TEXT)))
      {
        chilog(CRITICAL, "All other columns (%s) besides primary key must have no constraints and be either text or integer!", col->name);
        return CHIDB_EINVALIDSQL;
      }
    }
    _nCols++;
    col = col->next;
  }
  *nCols = _nCols;
  return CHIDB_OK;
}

static int chidb_stmt_codegen_simple_create_table(chidb_stmt *stmt, chisql_statement_t *sql_stmt)
{
  // Create_t *create = sql_stmt->stmt.create;
  Table_t *table = sql_stmt->stmt.create->table;
  int nCols;
  if (chidb_stmt_validate_simple_create_table(stmt, sql_stmt, &nCols) != CHIDB_OK)
  {
    chilog(CRITICAL, "INVALIDATED");
    return CHIDB_EINVALIDSQL;
  }
  chidb_dbm_op_t op_int = {Op_Integer, 1, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_int, 0);
  chidb_dbm_op_t op_openWrite = {Op_OpenWrite, 0, 0, 5, NULL};
  chidb_stmt_set_op(stmt, &op_openWrite, 1);
  chidb_dbm_op_t op_string_schematype = {Op_String, 5, 1, 0, "table"};
  chidb_stmt_set_op(stmt, &op_string_schematype, 2);
  chidb_dbm_op_t op_string_schemaname = {Op_String, strlen(table->name), 2, 0, table->name};
  chidb_stmt_set_op(stmt, &op_string_schemaname, 3);
  chidb_dbm_op_t op_string_associatedname = {Op_String, strlen(table->name), 3, 0, table->name};
  chidb_stmt_set_op(stmt, &op_string_associatedname, 4);
  chidb_dbm_op_t op_create_root = {Op_CreateTable, 4, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_create_root, 5);
  chidb_dbm_op_t op_string_sql = {Op_String, strlen(sql_stmt->text), 5, 0, sql_stmt->text};
  chidb_stmt_set_op(stmt, &op_string_sql, 6);
  chidb_dbm_op_t op_record = {Op_MakeRecord, 1, 5, 6, NULL};
  chidb_stmt_set_op(stmt, &op_record, 7);
  chidb_dbm_op_t op_int_key = {Op_Integer, stmt->db->nSchema + 1, 7, 0, NULL};
  chidb_stmt_set_op(stmt, &op_int_key, 8);
  chidb_dbm_op_t op_insert = {Op_Insert, 0, 6, 7, NULL};
  chidb_stmt_set_op(stmt, &op_insert, 9);
  chidb_dbm_op_t op_close = {Op_Close, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_close, 10);
  chidb_dbm_op_t op_halt = {Op_Halt, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_halt, 11);
  stmt->pc = 0;
  return CHIDB_OK;
}

static int chidb_stmt_codegen_create_index(chidb_stmt *stmt, chisql_statement_t *sql_stmt)
{
  Index_t *index = sql_stmt->stmt.create->index;
  if (schema_exists(stmt->db, index->name))
  {
    return CHIDB_EINVALIDSQL;
  }
  if (!schema_exists(stmt->db, index->table_name))
  {
    return CHIDB_EINVALIDSQL;
  }
  ChidbSchema table_schema;
  get_schema(stmt->db, index->table_name, &table_schema);
  int col_n = table_col_n(stmt->db, index->table_name, index->column_name);
  if (col_n == -1)
  {
    return CHIDB_EINVALIDSQL;
  }
  int j = table_col_type(stmt->db, index->table_name, index->column_name);
  if (j != TYPE_INT)
  {
    chilog(CRITICAL, "Column %s of table %s is of type %d, but indices can only be created on integer columns!",
           index->column_name, index->table_name, j);
    return CHIDB_EINVALIDSQL;
  }
  chilog(DEBUG, "Codegen for create index %s on %s %s, col type %d and col number %d",
         index->name, index->table_name, index->column_name, j, col_n);
  chidb_dbm_op_t op_int = {Op_Integer, table_schema.root_npage, 0, 0, NULL};
  chidb_dbm_op_t op_openReadTable = {Op_OpenRead, 0, 0, table_ncols(stmt->db, index->table_name), NULL};
  chidb_dbm_op_t op_createIndex = {Op_CreateIndex, 1, 0, 0, NULL};
  chidb_dbm_op_t op_openWriteIndex = {Op_OpenWrite, 1, 1, 0, NULL};
  chidb_dbm_op_t op_rewindTable = {Op_Rewind, 0, 9, 0, NULL};
  chidb_dbm_op_t op_key = {Op_Key, 0, 3, 0, NULL};
  chidb_dbm_op_t op_col = {Op_Column, 0, col_n, 2, NULL};
  chidb_dbm_op_t op_insertIndex = {Op_IdxInsert, 1, 2, 3, NULL};
  chidb_dbm_op_t op_next = {Op_Next, 0, 5, 0, NULL};
  chidb_dbm_op_t op_close0 = {Op_Close, 0, 0, 0, NULL};
  chidb_dbm_op_t op_close1 = {Op_Close, 1, 0, 0, NULL};
  chidb_dbm_op_t op_halt = {Op_Halt, 0, 0, 0, NULL};

  chidb_stmt_set_op(stmt, &op_int, 0);
  chidb_stmt_set_op(stmt, &op_openReadTable, 1);
  chidb_stmt_set_op(stmt, &op_createIndex, 2);
  chidb_stmt_set_op(stmt, &op_openWriteIndex, 3);
  chidb_stmt_set_op(stmt, &op_rewindTable, 4);
  chidb_stmt_set_op(stmt, &op_key, 5);
  chidb_stmt_set_op(stmt, &op_col, 6);
  chidb_stmt_set_op(stmt, &op_insertIndex, 7);
  chidb_stmt_set_op(stmt, &op_next, 8);
  chidb_stmt_set_op(stmt, &op_close0, 9);
  chidb_stmt_set_op(stmt, &op_close1, 10);
  chidb_stmt_set_op(stmt, &op_halt, 11);
  stmt->pc = 0;
  return CHIDB_OK;
}
