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

static int check_cols_exist(chidb_stmt *stmt, char *table_name, Expression_t *cols, int nCols);

static int chidb_stmt_validate_schema_exists(chidb_stmt *stmt, SRA_Table_t *sra_table, int *root_npage)
{
  npage_t _root_npage = schema_root_page(stmt->db, sra_table->ref->table_name);
  if (_root_npage == 0)
  {
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
      chilog(INFO, "Pkey %d in all", i);
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
  int cols_exist = check_cols_exist(stmt, sra_table.ref->table_name, cols, _nCols);
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
    chilog(INFO, "Validating project all");
    return chidb_stmt_validate_project_all_cols(stmt, sql_stmt, nCols, pkey_n);
  }
  else
  {
    chilog(INFO, "Validating project selected");
    return chidb_stmt_validate_project_selected_cols(stmt, sql_stmt, nCols, pkey_n);
  }
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
    return CHIDB_EINVALIDSQL;
  }
  return CHIDB_OK;
}

// static int chidb_stmt_validate(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int *nCols, int *root_npage)
// {
//   if (sql_stmt->type == STMT_SELECT && sql_stmt->stmt.select->t == SRA_PROJECT &&
//       sql_stmt->stmt.select->project.sra->t == SRA_SELECT &&
//       sql_stmt->stmt.select->project.sra->select.sra->t == SRA_TABLE)
//   {
//     if(chidb_stmt_validate_project_cols(stmt, sql_stmt, nCols, ))
//   }
// }

static int chidb_stmt_codegen_simple_select(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols, int pkey_n, int root_npage);

static int chidb_stmt_codegen_simple_select_where(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols, int pkey_n, int root_npage);

int chidb_stmt_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt)
{
  chilog(INFO, "ENTER CODEGEN BASE");
  if (stmt->ops == NULL)
  {
    chilog(INFO, "null ops: initalizing.");
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
    chilog(INFO, "Validating schema %s exists", sra_table.ref->table_name);
    if (chidb_stmt_validate_schema_exists(stmt, &sra_table, &root_npage) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    chilog(INFO, "Validating project cols");
    if (chidb_stmt_validate_project_cols(stmt, sql_stmt, &nCols, &pkey_n) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    chilog(INFO, "Validating simple select");
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
    if (chidb_stmt_validate_schema_exists(stmt, &sra_table, &root_npage) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    if (chidb_stmt_validate_project_cols(stmt, sql_stmt, &nCols, &pkey_n) != CHIDB_OK)
    {
      return CHIDB_EINVALIDSQL;
    }
    return chidb_stmt_codegen_simple_select(stmt, sql_stmt, nCols, pkey_n, root_npage);
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

static int check_cols_exist(chidb_stmt *stmt, char *table_name, Expression_t *cols, int nCols)
{
  Expression_t *curr_col = cols;
  while (curr_col != NULL)
  {
    nCols++;
    if (table_col_exists(stmt->db, table_name, curr_col->expr.term.ref->columnName) == 0)
    {
      return 0;
    }
    curr_col = curr_col->next;
  }
  chilog(INFO, " verified cols");
  return 1;
}

// fills instructions addr_start thru addr_start + nCols + 1.
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
  chilog(INFO, "Generated instructions for cols");
  return CHIDB_OK;
}

static int chidb_stmt_codegen_simple_select(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols, int pkey_n, int root_npage)
{
  chilog(INFO, "Enter code gen for select without where clause %s", sql_stmt->text);
  SRA_t *select = sql_stmt->stmt.select;
  SRA_Project_t sra_project = select->project;
  SRA_Table_t sra_table = sra_project.sra->table;
  ChidbSchema schema;
  get_schema(stmt->db, sra_table.ref->table_name, &schema);
  Expression_t *cols = sra_project.expr_list;
  if (strcmp(cols->expr.term.ref->columnName, "*") == 0)
  {
    chilog(INFO, "Select * detected.");
    int cols_a[nCols];
    for (int i = 0; i < nCols; i++)
    {
      cols_a[i] = i;
    }
    simple_col_codegen(stmt, 3, 0, cols_a, nCols, 3, 1, pkey_n);
  }
  else
  {
    chilog(INFO, "specified cols");
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
  chilog(INFO, "%d cols", nCols);
  chidb_dbm_op_t op_int = {Op_Integer, root_npage, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_int, 0);
  chidb_dbm_op_t op_openRead = {Op_OpenRead, 0, 0, nCols, NULL};
  chidb_stmt_set_op(stmt, &op_openRead, 1);
  chidb_dbm_op_t op_rewind = {Op_Rewind, 0, 3 + nCols + 2, 0, NULL};
  chidb_stmt_set_op(stmt, &op_rewind, 2);
  chidb_dbm_op_t op_close = {Op_Close, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_close, 3 + nCols + 2);
  chidb_dbm_op_t op_halt = {Op_Halt, 0, 0, 0, NULL};
  chidb_stmt_set_op(stmt, &op_halt, 3 + nCols + 3);
  stmt->pc = 0;
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

static int chidb_stmt_codegen_simple_select_where(chidb_stmt *stmt, chisql_statement_t *sql_stmt, int nCols, int pkey_n, int root_npage)
{
  chilog(INFO, "Enter codegen for select with where clause %s", sql_stmt->text);
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
    chilog(INFO, "Select * detected");
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
    chilog(INFO, "specified cols");
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
  chilog(INFO, "%d cols", nCols);
  chidb_dbm_op_t op_cmp = {simple_cmp_condtype_opcode(sra_select.cond->t), 1, 7, 2, NULL};
  chidb_stmt_set_op(stmt, &op_cmp, 5);
  chidb_dbm_op_t op_unconditional_jump = {Op_Eq, 1, 7 + nCols + 1, 1, NULL};
  chidb_stmt_set_op(stmt, &op_unconditional_jump, 6);
  chidb_dbm_op_t op_openRead = {Op_OpenRead, 0, 0, nCols, NULL};
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
