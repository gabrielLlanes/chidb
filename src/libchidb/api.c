/*
 *  chidb - a didactic relational database management system
 *
 * This module provides the chidb API.
 *
 * For more details on what each function does, see the chidb Architecture
 * document, or the chidb.h header file.
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

#include <stdlib.h>
#include <chidb/chidb.h>
#include "dbm.h"
#include "btree.h"
#include "record.h"
#include "util.h"
#include "chidbInt.h"

/* Implemented in codegen.c */
int chidb_stmt_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt);

/* Implemented in optimizer.c */
int chidb_stmt_optimize(chidb *db,
												chisql_statement_t *sql_stmt,
												chisql_statement_t **sql_stmt_opt);

/* your code */

static int realloc_schema(chidb *db, int n)
{
	if (n > db->nSchema)
	{
		chilog(INFO, "Reallocating schema: %d.", n);
		db->schema_list = realloc(db->schema_list, n * sizeof(ChidbSchema));
	}
}

int chidb_open(const char *file, chidb **db)
{
	chilog_setloglevel(INFO);
	*db = malloc(sizeof(chidb));
	if (*db == NULL)
		return CHIDB_ENOMEM;
	chidb_Btree_open(file, *db, &(*db)->bt);

	/* Additional initialization code goes here */
	// load database schema into the chidb struct.
	(*db)->schema_list = malloc(sizeof(ChidbSchema));
	(*db)->nSchema = 0;
	chidb_dbm_cursor_t *cursor;
	chidb_Cursor_open(&cursor, CURSOR_READ, (*db)->bt, 1, 5);
	cursor_node_entry entry = cursor->node_entries[cursor->nNodes - 1];
	chilog(INFO, "Cursor: %d type, key %d", cursor->tree_type, cursor->curr_key);
	chidb_Cursor_rewind(cursor);
	int nSchema = 0;
	if (!(cursor->nNodes == 1 && cursor->node_entries[0].node->n_cells == 0))
	{
		do
		{
			BTreeCell *cell;
			chidb_Cursor_get(cursor, &cell);
			// chilog(INFO, "GOT CELL type %d, KEY %d", cell->type, cell->key);
			uint32_t type;
			uint32_t offset;
			uint8_t *data = cell->fields.tableLeaf.data;

			// get column 4, which holds the sql statement
			getRecordCol(data, 4, &type, &offset);
			uint32_t len = (type - 13) / 2;
			char *sql = malloc(len + 1);
			memcpy(sql, data + offset, len);
			sql[len] = '\0';
			chisql_statement_t *stmt;

			chisql_parser(sql, &stmt);
			Create_t *create = stmt->stmt.create;
			ChidbSchema curr_schema;
			curr_schema.sql = sql;

			// get column 3, which holds the root page number of the table/index
			getRecordCol(data, 3, &type, &offset);
			curr_schema.root_npage = get4byte(data + offset);
			curr_schema.type = create->t;
			if (curr_schema.type == CREATE_INDEX)
			{
				curr_schema.index = create->index;
				curr_schema.name = create->index->name;
				curr_schema.assoc_table_name = create->index->table_name;
				chilog(INFO, "SCHEMA %d, INDEX %s: root %d, assoc %s, sql %s",
							 nSchema, curr_schema.name, curr_schema.root_npage, curr_schema.assoc_table_name, curr_schema.sql);
			}
			else if (curr_schema.type == CREATE_TABLE)
			{
				curr_schema.table = create->table;
				curr_schema.name = create->table->name;
				curr_schema.assoc_table_name = create->table->name;

				// debugging logging
				chilog(INFO, "SCHEMA %d, TABLE %s: root %d, assoc %s, sql %s",
							 nSchema, curr_schema.name, curr_schema.root_npage, curr_schema.assoc_table_name, curr_schema.sql);
				Column_t *currcol = curr_schema.table->columns;
				int cols = 1;
				while (currcol != NULL)
				{
					currcol = currcol->next;
				}
			}
			realloc_schema(*db, nSchema + 1);
			(*db)->schema_list[nSchema] = curr_schema;
			(*db)->nSchema += 1;
			nSchema += 1;
		} while (chidb_Cursor_next(cursor) != CHIDB_CURSOR_LAST_ENTRY);
	}
	else
	{
		chilog(WARNING, "Empty Btree!");
	}
	// chilog(INFO, "Exists: numbers %d, idxNumbers %d, ahaha %d",
	// 			 schema_exists(*db, "numbers"), schema_exists(*db, "idxNumbers"), schema_exists(*db, "ahaha"));
	// chilog(INFO, "Root pages: numbers %d, idxNumbers %d, ahaha %d",
	// 			 schema_root_page(*db, "numbers"), schema_root_page(*db, "idxNumbers"), schema_root_page(*db, "ahaha"));
	// chilog(INFO, "Cols exist: code %d, textcode %d, altcode %d, ahaha %d",
	// 			 table_col_exists(*db, "numbers", "code"),
	// 			 table_col_exists(*db, "numbers", "textcode"),
	// 			 table_col_exists(*db, "numbers", "altcode"),
	// 			 table_col_exists(*db, "numbers", "ahaha"));
	// chilog(INFO, "Cols types: code %d, textcode %d, altcode %d, ahaha %d",
	// 			 table_col_type(*db, "numbers", "code"),
	// 			 table_col_type(*db, "numbers", "textcode"),
	// 			 table_col_type(*db, "numbers", "altcode"),
	// 			 table_col_type(*db, "numbers", "ahaha"));
	return CHIDB_OK;
}

int chidb_close(chidb *db)
{
	chidb_Btree_close(db->bt);
	free(db);

	/* Additional cleanup code goes here */

	return CHIDB_OK;
}

int chidb_prepare(chidb *db, const char *sql, chidb_stmt **stmt)
{
	int rc;
	chisql_statement_t *sql_stmt, *sql_stmt_opt;

	*stmt = malloc(sizeof(chidb_stmt));

	rc = chidb_stmt_init(*stmt, db);

	if (rc != CHIDB_OK)
	{
		free(*stmt);
		return rc;
	}

	rc = chisql_parser(sql, &sql_stmt);

	if (rc != CHIDB_OK)
	{
		free(*stmt);
		return rc;
	}

	rc = chidb_stmt_optimize((*stmt)->db, sql_stmt, &sql_stmt_opt);

	if (rc != CHIDB_OK)
	{
		free(*stmt);
		return rc;
	}

	rc = chidb_stmt_codegen(*stmt, sql_stmt_opt);

	free(sql_stmt_opt);

	(*stmt)->explain = sql_stmt->explain;

	return rc;
}

int chidb_step(chidb_stmt *stmt)
{
	if (stmt->explain)
	{
		if (stmt->pc == stmt->endOp)
			return CHIDB_DONE;
		else
		{
			stmt->pc++;
			return CHIDB_ROW;
		}
	}
	else
		return chidb_stmt_exec(stmt);
}

int chidb_finalize(chidb_stmt *stmt)
{
	return chidb_stmt_free(stmt);
}

int chidb_column_count(chidb_stmt *stmt)
{
	if (stmt->explain)
		return 6;
	else
		return stmt->nCols;
}

int chidb_column_type(chidb_stmt *stmt, int col)
{
	if (stmt->explain)
	{
		chidb_dbm_op_t *op = &stmt->ops[stmt->pc - 1];

		switch (col)
		{
		case 0:
			return SQL_INTEGER_4BYTE;
		case 1:
			return 2 * strlen(opcode_to_str(op->opcode)) + SQL_TEXT;
		case 2:
		case 3:
		case 4:
			return SQL_INTEGER_4BYTE;
		case 5:
			if (op->p4 == NULL)
				return SQL_NULL;
			else
				return 2 * strlen(op->p4) + SQL_TEXT;
		default:
			return SQL_NOTVALID;
		}
	}
	else
	{
		if (col < 0 || col >= stmt->nCols)
			return SQL_NOTVALID;
		else
		{
			chidb_dbm_register_t *r = &stmt->reg[stmt->startRR + col];

			switch (r->type)
			{
			case REG_UNSPECIFIED:
			case REG_BINARY:
				return SQL_NOTVALID;
				break;
			case REG_NULL:
				return SQL_NULL;
				break;
			case REG_INT32:
				return SQL_INTEGER_4BYTE;
				break;
			case REG_STRING:
				return 2 * strlen(r->value.s) + SQL_TEXT;
				break;
			default:
				return SQL_NOTVALID;
			}
		}
	}
}

const char *chidb_column_name(chidb_stmt *stmt, int col)
{
	if (stmt->explain)
	{
		switch (col)
		{
		case 0:
			return "addr";
		case 1:
			return "opcode";
		case 2:
			return "p1";
		case 3:
			return "p2";
		case 4:
			return "p3";
		case 5:
			return "p4";
		default:
			return NULL;
		}
	}
	else
	{
		if (col < 0 || col >= stmt->nCols)
			return NULL;
		else
			return stmt->cols[col];
	}
}

int chidb_column_int(chidb_stmt *stmt, int col)
{
	if (stmt->explain)
	{
		chidb_dbm_op_t *op = &stmt->ops[stmt->pc - 1];

		switch (col)
		{
		case 0:
			return stmt->pc - 1;
		case 1:
			return 0; /* Undefined */
		case 2:
			return op->p1;
		case 3:
			return op->p2;
		case 4:
			return op->p3;
		case 5:
			return 0; /* Undefined */
		default:
			return 0; /* Undefined */
		}
	}
	else
	{
		if (col < 0 || col >= stmt->nCols)
		{
			/* Undefined behaviour */
			return 0;
		}
		else
		{
			chidb_dbm_register_t *r = &stmt->reg[stmt->startRR + col];

			if (r->type != REG_INT32)
			{
				/* Undefined behaviour */
				return 0;
			}
			else
			{
				return r->value.i;
			}
		}
	}
}

const char *chidb_column_text(chidb_stmt *stmt, int col)
{
	if (stmt->explain)
	{
		chidb_dbm_op_t *op = &stmt->ops[stmt->pc - 1];

		switch (col)
		{
		case 0:
			return NULL; /* Undefined */
		case 1:
			return opcode_to_str(op->opcode);
		case 2:
			return NULL; /* Undefined */
		case 3:
			return NULL; /* Undefined */
		case 4:
			return NULL; /* Undefined */
		case 5:
			return op->p4;
		default:
			return 0; /* Undefined */
		}
	}
	else
	{
		if (col < 0 || col >= stmt->nCols)
		{
			/* Undefined behaviour */
			return NULL;
		}
		else
		{
			chidb_dbm_register_t *r = &stmt->reg[stmt->startRR + col];

			if (r->type != REG_STRING)
			{
				/* Undefined behaviour */
				return NULL;
			}
			else
			{
				return r->value.s;
			}
		}
	}
}
