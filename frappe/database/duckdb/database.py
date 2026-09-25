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


# Same batch size `sync_using_pyarrow` streams full-table resyncs in.
CDC_BATCH_SIZE = 204800


def _timedelta_to_time(td):
	from datetime import time

	total_seconds = int(td.total_seconds()) % 86400
	hours, remainder = divmod(total_seconds, 3600)
	minutes, seconds = divmod(remainder, 60)
	return time(hours, minutes, seconds, td.microseconds)


def _flush_cdc_batch(conn, table_name, schema, time_columns, batch: dict):
	"""Apply one table's buffered CDC events in bulk.

	Mirrors `sync_using_pyarrow`: a single delete for every row touched by the batch,
	followed by zero-copy arrow inserts, instead of a delete+insert round trip per row.
	`batch` maps row name -> new values for an upsert, or None for a delete; only the
	last event seen for a given name in this batch is applied, which is safe because
	CDC upserts are themselves delete-then-insert (idempotent, order only matters
	across, not within, a coalesced batch).
	"""
	from datetime import timedelta
	from decimal import Decimal

	import pyarrow as pa

	if not batch:
		return

	names = list(batch.keys())
	placeholders = ", ".join(["?"] * len(names))
	conn.execute(f'delete from "{table_name}" where "name" in ({placeholders})', names)

	upserts = [values for values in batch.values() if values is not None]
	if not upserts:
		return

	# Binlog rows carry MariaDB DECIMAL columns as `decimal.Decimal`, which pyarrow
	# refuses to narrow to the float64 arrow type implicitly.
	for row in upserts:
		for col, value in row.items():
			if isinstance(value, Decimal):
				row[col] = float(value)
			elif col in time_columns and isinstance(value, timedelta):
				row[col] = _timedelta_to_time(value)

	field_list = ", ".join(f'"{c}"' for c in schema.names)
	for start in range(0, len(upserts), CDC_BATCH_SIZE):
		chunk = upserts[start : start + CDC_BATCH_SIZE]
		arrow_table = pa.Table.from_batches([pa.RecordBatch.from_pylist(chunk, schema=schema)])
		conn.register("arrow_table", arrow_table)
		conn.execute(
			f'insert into "{table_name}" ({field_list}) select {field_list} from arrow_table;'
		).fetchall()
		conn.unregister("arrow_table")
		del arrow_table


class BinlogReconnectLimitExceeded(Exception):
	pass


def cdc():
	import pyarrow as pa
	import pymysql
	from pymysqlreplication import BinLogStreamReader
	from pymysqlreplication.row_event import DeleteRowsEvent, TableMapEvent, UpdateRowsEvent, WriteRowsEvent

	from frappe.database import get_ducklake

	cs = {
		"host": frappe.conf.db_host,
		"port": frappe.conf.db_port,
		"user": frappe.conf.db_user,
		"passwd": frappe.conf.db_password,
	}

	# `python-mysql-replication` retries a dropped stream connection immediately and
	# indefinitely (no cap, no backoff), so a persistent connection failure otherwise
	# floods the log forever instead of surfacing. Bound it ourselves.
	MAX_RECONNECT_ATTEMPTS = 5

	tables = frappe.db.get_all(
		"DuckDB Sync Item", filters={"synced": 1}, fields="name, table, gtid_binlog_pos"
	)

	conn = get_ducklake()
	try:
		for x in tables:
			table_name = "tab" + x.table
			columns = {row[0] for row in conn.sql(f'describe "{table_name}"')}

			duck_tb = DuckDBTable(x.table)
			schema = pa.schema([f for f in duck_tb.get_arrow_schema() if f.name in columns])
			time_columns = [
				name
				for name, dtype in zip(schema.names, schema.types, strict=False)
				if pa.types.is_time(dtype)
			]

			batch: dict[str, dict | None] = {}

			attempts = 0

			def connect_with_reconnect_limit(**settings):
				nonlocal attempts
				attempts += 1
				if attempts > MAX_RECONNECT_ATTEMPTS:
					raise BinlogReconnectLimitExceeded(
						f"Giving up on binlog stream for {table_name} after {MAX_RECONNECT_ATTEMPTS} "
						"consecutive connection failures"
					)
				return pymysql.connect(**settings)

			binlog_pos = frappe.db.sql("select @@gtid_binlog_pos;", pluck=True)[0]
			stream = BinLogStreamReader(
				connection_settings=cs,
				server_id=3,
				blocking=False,
				is_mariadb=True,
				auto_position=x.gtid_binlog_pos,
				slave_heartbeat=10,
				use_column_name_cache=True,
				only_events=[UpdateRowsEvent, DeleteRowsEvent, TableMapEvent, WriteRowsEvent],
				only_schemas=[frappe.conf.db_name],
				only_tables=[table_name],
				pymysql_wrapper=connect_with_reconnect_limit,
			)
			try:
				for event in stream:
					if isinstance(event, WriteRowsEvent):
						for row in event.rows:
							values = _filter_row_values(columns, row["values"])
							if values.get("name"):
								batch[values["name"]] = values
					elif isinstance(event, UpdateRowsEvent):
						for row in event.rows:
							values = _filter_row_values(columns, row["after_values"])
							if values.get("name"):
								batch[values["name"]] = values
					elif isinstance(event, DeleteRowsEvent):
						for row in event.rows:
							if name := row["values"].get("name"):
								batch[name] = None
					# TableMapEvent carries schema info only, no row data - nothing to write.

					if len(batch) >= CDC_BATCH_SIZE:
						_flush_cdc_batch(conn, table_name, schema, time_columns, batch)
						batch.clear()
			except BinlogReconnectLimitExceeded:
				frappe.log_error(title="DuckDB CDC binlog reconnect limit exceeded", message=table_name)
			finally:
				_flush_cdc_batch(conn, table_name, schema, time_columns, batch)
				stream.close()
				frappe.db.set_value("DuckDB Sync Item", x.name, "gtid_binlog_pos", binlog_pos)
	finally:
		conn.close()


def count_gle():
	from frappe.database import get_ducklake

	d = get_ducklake()
	print(d.sql('select count(*) from "tabGL Entry";'))
