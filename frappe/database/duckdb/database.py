from duckdb import DuckDBPyRelation

import frappe
from frappe import qb
from frappe.database.database import Database
from frappe.database.duckdb.schema import DuckDBTable


def get_type_map():
	return {
		"Currency": ("decimal", "21,9"),
		"Int": ("int", ""),
		"Long Int": ("bigint", "20"),
		"Float": ("decimal", "21,9"),
		"Percent": ("decimal", "21,9"),
		"Check": ("tinyint", ""),
		"Small Text": ("text", ""),
		"Long Text": ("text", ""),
		"Code": ("text", ""),
		"Text Editor": ("text", ""),
		"Markdown Editor": ("text", ""),
		"HTML Editor": ("text", ""),
		"Date": ("date", ""),
		"Datetime": ("datetime", ""),
		"Time": ("time", ""),
		"Text": ("text", ""),
		"Data": ("varchar", frappe.db.VARCHAR_LEN),
		"Link": ("varchar", frappe.db.VARCHAR_LEN),
		"Dynamic Link": ("varchar", frappe.db.VARCHAR_LEN),
		"Password": ("text", ""),
		"Select": ("varchar", frappe.db.VARCHAR_LEN),
		"Rating": ("decimal", "3,2"),
		"Read Only": ("varchar", frappe.db.VARCHAR_LEN),
		"Attach": ("text", ""),
		"Attach Image": ("text", ""),
		"Signature": ("text", ""),
		"Color": ("varchar", frappe.db.VARCHAR_LEN),
		"Barcode": ("text", ""),
		"Geolocation": ("text", ""),
		"Duration": ("decimal", "21,9"),
		"Icon": ("varchar", frappe.db.VARCHAR_LEN),
		"Phone": ("varchar", frappe.db.VARCHAR_LEN),
		"Autocomplete": ("varchar", frappe.db.VARCHAR_LEN),
		"JSON": ("json", ""),
	}


def get_pyarrow_type_map():
	import pyarrow as pa

	return {
		"Currency": pa.float64(),
		"Int": pa.int32(),
		"Long Int": pa.int64(),
		"Float": pa.float64(),
		"Percent": pa.float64(),
		"Check": pa.int8(),
		"Small Text": pa.string(),
		"Long Text": pa.string(),
		"Code": pa.string(),
		"Text Editor": pa.string(),
		"Markdown Editor": pa.string(),
		"HTML Editor": pa.string(),
		"Date": pa.date32(),
		"Datetime": pa.timestamp("us"),
		"Time": pa.time64("us"),
		"Text": pa.string(),
		"Data": pa.string(),
		"Link": pa.string(),
		"Dynamic Link": pa.string(),
		"Password": pa.string(),
		"Select": pa.string(),
		"Rating": pa.float64(),
		"Read Only": pa.string(),
		"Attach": pa.string(),
		"Attach Image": pa.string(),
		"Signature": pa.string(),
		"Color": pa.string(),
		"Barcode": pa.string(),
		"Geolocation": pa.string(),
		"Duration": pa.float64(),
		"Icon": pa.string(),
		"Phone": pa.string(),
		"Autocomplete": pa.string(),
		"JSON": pa.large_string(),
	}


def get_latest_sync(doctype: str | None = None):
	if doctype:
		if latest_sync := frappe.db.get_all(
			"DuckDB Sync", filters={"doc_type": doctype}, pluck="name", order_by="creation desc", limit=1
		):
			return frappe.get_doc("DuckDB Sync", latest_sync[0]).get_duckdb_conn()
	return None


class DuckDBDatabase:
	"""Wraps a DuckDB connection so fetch results automatically convert Decimal to float."""

	def __init__(self, conn):
		self._conn = conn

	def __getattr__(self, name):
		return getattr(self._conn, name)

	def execute(self, query, parameters=None):
		rel = self._conn.execute(query, parameters) if parameters is not None else self._conn.execute(query)
		return DuckDBRelation(rel)

	def sql(self, query, as_dict=False, pluck=None):
		relation = self._conn.sql(query)
		result = ()
		if isinstance(relation, DuckDBPyRelation):
			columns = relation.columns
			partial = DuckDBRelation(relation).fetchall()

			if pluck and not as_dict:
				columns = [relation.columns[0]]
				partial = [(x[0]) for x in partial]

			if as_dict:
				result = [dict(zip(columns, row, strict=False)) for row in partial]
			else:
				result = partial

		return result


class DuckDBRelation:
	"""Wraps a DuckDB relation to convert Decimal results to float on fetch."""

	def __init__(self, rel):
		self._rel = rel

	def __getattr__(self, name):
		return getattr(self._rel, name)

	def fetchall(self):
		from decimal import Decimal

		return [tuple(float(v) if isinstance(v, Decimal) else v for v in row) for row in self._rel.fetchall()]

	def fetchone(self):
		from decimal import Decimal

		row = self._rel.fetchone()
		if row is None:
			return None
		return tuple(float(v) if isinstance(v, Decimal) else v for v in row)

	def fetchmany(self, size=1):
		from decimal import Decimal

		return [
			tuple(float(v) if isinstance(v, Decimal) else v for v in row) for row in self._rel.fetchmany(size)
		]


def start_duckdb_sync():
	_dt = qb.DocType("Doctype To Sync")
	to_sync = (
		qb.from_(_dt)
		.select(_dt.doc_type)
		.distinct()
		.where(_dt.parenttype.eq("Report") & _dt.parentfield.eq("doctype_to_sync"))
		.run(pluck="doc_type")
	)
	for x in to_sync:
		doc = frappe.get_doc(
			{
				"doctype": "DuckDB Sync",
				"doc_type": x,
			}
		).insert()
		doc.submit()


def _filter_row_values(columns, values: dict) -> dict:
	"""Keep only the values whose column actually exists on the DuckDB table."""
	return {k: v for k, v in values.items() if k in columns}


def _upsert_duckdb_row(conn, table_name: str, values: dict):
	"""Delete-then-insert so this is safe to replay (no PK/UNIQUE constraint exists on `name`)."""
	if not values or "name" not in values:
		return

	conn.execute(f'delete from "{table_name}" where "name" = ?', [values["name"]])

	columns = list(values.keys())
	col_list = ", ".join(f'"{c}"' for c in columns)
	placeholders = ", ".join(["?"] * len(columns))
	conn.execute(
		f'insert into "{table_name}" ({col_list}) values ({placeholders})',
		list(values.values()),
	)


def _delete_duckdb_row(conn, table_name: str, values: dict):
	if not values or "name" not in values:
		return
	conn.execute(f'delete from "{table_name}" where "name" = ?', [values["name"]])


def cdc():
	from pymysqlreplication import BinLogStreamReader
	from pymysqlreplication.row_event import DeleteRowsEvent, TableMapEvent, UpdateRowsEvent, WriteRowsEvent

	from frappe.database import get_ducklake

	cs = {
		"host": frappe.conf.db_host,
		"port": frappe.conf.db_port,
		"user": frappe.conf.db_user,
		"passwd": frappe.conf.db_password,
	}

	tables = frappe.db.get_all("DuckDB Sync Item", filters={"synced": 1}, fields="table, gtid_binlog_pos")

	conn = get_ducklake()
	try:
		for x in tables:
			table_name = "tab" + x.table
			columns = {row[0] for row in conn.sql(f'describe "{table_name}"')}

			stream = BinLogStreamReader(
				connection_settings=cs,
				server_id=3,
				blocking=False,
				is_mariadb=True,
				auto_position=x.gtid_binlog_pos,
				slave_heartbeat=10,
				only_events=[UpdateRowsEvent, DeleteRowsEvent, TableMapEvent, WriteRowsEvent],
				only_schemas=[frappe.conf.db_name],
				only_tables=[table_name],
			)
			for event in stream:
				if isinstance(event, WriteRowsEvent):
					for row in event.rows:
						_upsert_duckdb_row(conn, table_name, _filter_row_values(columns, row["values"]))
				elif isinstance(event, UpdateRowsEvent):
					for row in event.rows:
						_upsert_duckdb_row(conn, table_name, _filter_row_values(columns, row["after_values"]))
				elif isinstance(event, DeleteRowsEvent):
					for row in event.rows:
						_delete_duckdb_row(conn, table_name, row["values"])
				# TableMapEvent carries schema info only, no row data - nothing to write.
	finally:
		conn.close()
