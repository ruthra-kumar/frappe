import datetime
import json
import logging
from functools import cache
from importlib import reload
from pathlib import Path

import frappe
from frappe.model.naming import revert_series_if_last
from frappe.modules import load_doctype_module

logger = logging.getLogger(__name__)
testing_logger = logging.getLogger("frappe.testing.generators")

datetime_like_types = (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)

__all__ = [
	"get_modules",
	"get_missing_records_doctypes",
	"get_missing_records_module_overrides",
	"make_test_records",
	"make_test_records_for_doctype",
	"make_test_objects",
]


@cache
def get_modules(doctype):
	"""Get the modules for the specified doctype"""
	module = frappe.db.get_value("DocType", doctype, "module")
	try:
		test_module = load_doctype_module(doctype, module, "test_")
		if test_module:
			reload(test_module)
	except ImportError:
		test_module = None

	return module, test_module


# @cache - don't cache the recursion, code depends on its recurn value declining
def get_missing_records_doctypes(doctype):
	"""Get the dependencies for the specified doctype in a depth-first manner"""
	# If already visited in a prior run
	if doctype in frappe.local.test_objects:
		return []
	else:
		# Infinite recrusion guard (depth-first discovery)
		frappe.local.test_objects[doctype] = []

	module, test_module = get_modules(doctype)
	meta = frappe.get_meta(doctype)
	link_fields = meta.get_link_fields()

	for df in meta.get_table_fields():
		link_fields.extend(frappe.get_meta(df.options).get_link_fields())

	unique_doctypes = dict.fromkeys(df.options for df in link_fields if df.options != "[Select]")

	to_add, to_remove = get_missing_records_module_overrides(test_module)
	unique_doctypes.update(dict.fromkeys(to_add))
	if to_remove:
		unique_doctypes = {k: v for k, v in unique_doctypes.items() if k not in to_remove}

	# Recursive depth-first traversal
	result = []
	for dep_doctype in unique_doctypes:
		result.extend(get_missing_records_doctypes(dep_doctype))

	result.append(doctype)
	return result


def get_missing_records_module_overrides(module) -> [list, list]:
	to_add = []
	to_remove = []
	if hasattr(module, "test_dependencies"):
		from frappe.deprecation_dumpster import deprecation_warning

		deprecation_warning(
			"2024-10-09",
			"v17",
			"""test_dependencies was clarified to EXTRA_TEST_RECORD_DEPENDENCIES: run:
```bash
# Find Python files
find . -name "*.py" | while read -r file; do
    # Check if the file contains 'test_dependencies' at the module level
    if grep -q "^test_dependencies" "$file"; then
        # Replace 'test_dependencies' with 'EXTRA_TEST_RECORD_DEPENDENCIES'
        sed -i 's/^test_dependencies/EXTRA_TEST_RECORD_DEPENDENCIES/' "$file"
        echo "Updated $file"
    fi
done
```""",
		)
		to_add += module.test_dependencies

	if hasattr(module, "EXTRA_TEST_RECORD_DEPENDENCIES"):
		to_add += module.EXTRA_TEST_RECORD_DEPENDENCIES

	if hasattr(module, "test_ignore"):
		from frappe.deprecation_dumpster import deprecation_warning

		deprecation_warning(
			"2024-10-09",
			"v17",
			"""test_ignore was clarified to IGNORE_TEST_RECORD_DEPENDENCIES: run:
```bash
# Find Python files
find . -name "*.py" | while read -r file; do
    # Check if the file contains 'test_ignore' at the module level
    if grep -q "^test_ignore" "$file"; then
        # Replace 'test_ignore' with 'IGNORE_TEST_RECORD_DEPENDENCIES'
        sed -i 's/^test_ignore/IGNORE_TEST_RECORD_DEPENDENCIES/' "$file"
        echo "Updated $file"
    fi
done
```""",
		)
		to_remove += module.test_ignore

	if hasattr(module, "IGNORE_TEST_RECORD_DEPENDENCIES"):
		to_remove += module.IGNORE_TEST_RECORD_DEPENDENCIES

	return to_add, to_remove


# Test record generation


def _make_test_records(doctype, force=False, commit=False):
	"""Make test records for the specified doctype"""
	for _doctype in get_missing_records_doctypes(doctype):
		# Create all test records and yield
		res = list(_make_test_record(_doctype, force, commit))
		yield (_doctype, len(res))


def _make_test_record(doctype, force=False, commit=False):
	"""Make test records for the specified doctype"""

	module, test_module = get_modules(doctype)
	if hasattr(test_module, "_make_test_records"):
		yield from test_module._make_test_records()
	elif hasattr(test_module, "test_records"):
		yield from _make_test_objects(doctype, test_module.test_records, force, commit=commit)
	else:
		test_records = frappe.get_test_records(doctype)
		if test_records:
			yield from _make_test_objects(doctype, test_records, force, commit=commit)
		else:
			print_mandatory_fields(doctype)


def _make_test_objects(doctype, test_records=None, reset=False, commit=False):
	"""Generator function to make test objects"""
	# NOTE: We use file-based, per-site persistence visited log in order to not
	# create same records twice for on multiple test runs
	test_record_log_instance = TestRecordLog()
	if not reset and doctype in test_record_log_instance.get():
		yield from test_record_log_instance.yield_names(doctype)

	def revert_naming(d):
		if getattr(d, "naming_series", None):
			revert_series_if_last(d.naming_series, d.name)

	if test_records is None:
		test_records = frappe.get_test_records(doctype)

	for doc in test_records:
		if not reset:
			frappe.db.savepoint("creating_test_record")

		if not doc.get("doctype"):
			doc["doctype"] = doctype

		d = frappe.copy_doc(doc)

		if d.meta.get_field("naming_series"):
			if not d.naming_series:
				d.naming_series = "_T-" + d.doctype + "-"

		if doc.get("name"):
			d.name = doc.get("name")
		else:
			d.set_new_name()

		if frappe.db.exists(d.doctype, d.name) and not reset:
			frappe.db.rollback(save_point="creating_test_record")
			# do not create test records, if already exists
			continue

		# submit if docstatus is set to 1 for test record
		docstatus = d.docstatus

		d.docstatus = 0

		try:
			d.run_method("before_test_insert")
			d.insert(ignore_if_duplicate=True)

			if docstatus == 1:
				d.submit()

		except frappe.NameError:
			revert_naming(d)

		except Exception as e:
			if (
				d.flags.ignore_these_exceptions_in_test
				and e.__class__ in d.flags.ignore_these_exceptions_in_test
			):
				revert_naming(d)
			else:
				logger.debug(f"Error in making test record for {d.doctype} {d.name}")
				raise

		if commit:
			frappe.db.commit()

		frappe.local.test_objects[doctype].append(d.name)
		yield d.name

	test_record_log_instance.add(doctype, frappe.local.test_objects[doctype])


def print_mandatory_fields(doctype):
	"""Print mandatory fields for the specified doctype"""
	meta = frappe.get_meta(doctype)
	msg = f"Setup test records for: {doctype}\n"
	msg += f"Autoname '{meta.autoname or ''}'\n"
	mandatory_fields = meta.get("fields", {"reqd": 1})
	if mandatory_fields:
		msg += "Mandatory Fields\n"
		for d in mandatory_fields:
			msg += f" {d.parent} {d.fieldname} ({d.fieldtype})"
			if d.options:
				opts = d.options.splitlines()
				msg += f" opts: {','.join(opts)} \n"
			else:
				msg += "\n"
	logger.warning("-" * 60 + "\n" + msg)
	testing_logger.warning(" | ".join(msg.strip().splitlines()))


class TestRecordLog:
	def __init__(self):
		self.log_file = Path(frappe.get_site_path(".test_log"))
		self._log = None

	def get(self):
		if self._log is None:
			self._log = self._read_log()
		return self._log

	def yield_names(self, doctype):
		log = self.get()
		yield from log[doctype]

	def add(self, doctype, names):
		log = self.get()
		if doctype not in log:
			log[doctype] = names
			testing_logger.debug(f"{self.log_file}: test record creation persisted for {doctype}")
			self._write_log(log)

	def _read_log(self):
		if self.log_file.exists():
			with self.log_file.open() as f:
				return json.load(f)
		return {}

	def _write_log(self, log):
		with self.log_file.open("w") as f:
			json.dump(log, f)


def _after_install_clear_test_log():
	with open(frappe.get_site_path(".test_log"), "w") as f:
		f.write("{}")


def make_test_records(doctype, force=False, commit=False):
	return list(_make_test_records(doctype, force, commit))


def make_test_records_for_doctype(doctype, force=False, commit=False):
	return list(_make_test_record(doctype, force, commit))


def make_test_objects(doctype, test_records=None, reset=False, commit=False):
	return list(_make_test_objects(doctype, test_records, reset, commit))