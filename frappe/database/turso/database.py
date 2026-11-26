"""Turso (pyturso) backend.

Reuses the SQLite backend wholesale (Turso is SQLite-compatible, in-process, file based) and only
swaps the driver. pyturso has no ``create_function``, so the Python UDFs frappe's SQLite backend
registers (regexp, now, datediff, ...) are unavailable here.
"""

import re
from pathlib import Path

import turso

import frappe
from frappe.database.sqlite.database import SQLiteDatabase, SQLiteExceptionUtil

_CREATE_TABLE_RE = re.compile(
	r"""CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"((?:[^"]|"")+)"|`([^`]+)`|\[([^\]]+)\]|'([^']+)'|(\S+?))\s*(?:\(|AS\b)""",
	re.IGNORECASE,
)


def _declared_table_name(name: str, sql: str | None) -> str:
	match = _CREATE_TABLE_RE.match(sql or "")
	if match:
		declared = next(g for g in match.groups() if g is not None).replace('""', '"')
		if declared.lower() == name.lower():
			return declared
	return name


class TursoExceptionUtil(SQLiteExceptionUtil):
	ProgrammingError = turso.ProgrammingError
	TableMissingError = turso.OperationalError
	OperationalError = turso.OperationalError
	InternalError = turso.InternalError
	SQLError = turso.OperationalError
	DataError = turso.DataError

	@staticmethod
	def is_deadlocked(e) -> bool:
		return isinstance(e, frappe.QueryDeadlockError)

	@classmethod
	def is_timedout(cls, e) -> bool:
		return isinstance(e, frappe.QueryTimeoutError) or "database is locked" in str(e)

	@staticmethod
	def is_primary_key_violation(e) -> bool:
		return "UNIQUE constraint failed" in str(e) or "PRIMARY KEY" in str(e)

	@staticmethod
	def is_unique_key_violation(e) -> bool:
		return "UNIQUE constraint failed" in str(e)

	@staticmethod
	def is_interface_error(e) -> bool:
		return isinstance(e, turso.InterfaceError)


class TursoDatabase(TursoExceptionUtil, SQLiteDatabase):
	def get_connection(self, read_only: bool = False):
		from zoneinfo import ZoneInfo

		if not hasattr(self, "_session_time_zone"):
			self._session_time_zone = ZoneInfo("UTC")
		return self.create_connection(read_only)

	def create_connection(self, read_only: bool = False):
		db_path = self.get_db_path()
		db_path.parent.mkdir(parents=True, exist_ok=True)
		# multiprocess_wal lets several processes (web, workers, console) open the same file;
		# every process must pass it. Not combinable with MVCC (BEGIN CONCURRENT).
		return turso.connect(str(db_path), experimental_features="index_method,multiprocess_wal")

	def get_db_path(self) -> Path:
		return Path(frappe.get_site_path()) / "db" / f"{self.cur_db_name}.db"

	def setup_type_map(self):
		super().setup_type_map()
		# internal flag: keep the sqlite code paths that key off db.db_type
		self.db_type = "sqlite"

	def set_execution_timeout(self, seconds: float):
		pass

	def get_execution_timeout(self) -> float:
		return 0.0

	def begin(self, *, read_only=False, defer_writer_lock=False):
		# Same as the SQLite version, but catches turso's exception type and never opens a
		# separate read-only connection (pyturso has no mode=ro URI support).
		try:
			self.sql("BEGIN")
		except turso.OperationalError as e:
			if not self.is_nested_transaction_error(e):
				raise

	def start_next_transaction(self, callbacks) -> None:
		# SQLite's version uses Connection.total_changes to drop the writer lock after no-op
		# callbacks; pyturso doesn't expose it.
		self.begin()
		self.value_cache.clear()
		self.run_after_transaction_callbacks(callbacks)

	def create_global_search_table(self):
		# Turso has no FTS5; its native FTS is an experimental index method with different
		# query syntax (`fts_match`/`fts_score`), so frappe.utils.global_search needs porting
		# before search works. Here we only create the storage + index.
		if "__global_search" not in self.get_tables():
			self.sql_ddl(
				"""CREATE TABLE __global_search (
				doctype TEXT, name TEXT, title TEXT, content TEXT, route TEXT, published INTEGER
				)"""
			)
			self.sql_ddl("CREATE INDEX __global_search_fts ON __global_search USING fts(title, content)")

	def get_tables(self, cached=True):
		"""Turso 0.8 lowercases names in sqlite_master; recover the declared case from `sql`."""
		transaction_has_schema_changes = getattr(self, "_transaction_has_schema_changes", False)
		to_query = not cached or transaction_has_schema_changes

		if cached and not transaction_has_schema_changes:
			tables = frappe.client_cache.get_value("db_tables")
			to_query = not tables

		if to_query:
			rows = self.sql(
				"SELECT name, sql FROM sqlite_master WHERE type='table'", _skip_sqlite_transpilation=True
			)
			tables = [_declared_table_name(name, sql) for name, sql in rows]
			if not transaction_has_schema_changes:
				frappe.client_cache.set_value("db_tables", tables)

		return tables

	def count(self, dt, filters=None, debug=False, cache=False, distinct: bool = True):
		# Turso 0.8 crashes on `SELECT DISTINCT COUNT(*)` (HashDistinct unresolved label). DISTINCT
		# over a single aggregate row is a no-op, so drop it.
		return super().count(dt, filters=filters, debug=debug, cache=cache, distinct=False)
