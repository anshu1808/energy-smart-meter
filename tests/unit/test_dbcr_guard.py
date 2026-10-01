import pathlib
import subprocess
import sys

GUARD = str(pathlib.Path(__file__).resolve().parents[2] / "ci" / "dbcr_guard.py")


def run(sql, confirm=""):
    env = {"CONFIRM_DESTRUCTIVE": confirm, "PATH": ""}
    return subprocess.run([sys.executable, GUARD], input=sql, text=True, capture_output=True, env=env)


def test_create_table_allowed():
    assert run("CREATE TABLE dev_energy.bronze.t (id STRING);").returncode == 0


def test_create_schema_blocked():
    r = run("CREATE SCHEMA dev_energy.new;")
    assert r.returncode == 1 and "BLOCKED" in r.stderr


def test_drop_held_until_confirmed():
    assert run("DROP TABLE dev_energy.bronze.t;").returncode == 1
    assert run("DROP TABLE dev_energy.bronze.t;", confirm="true").returncode == 0


def test_liquibase_bookkeeping_ignored():
    sql = "-- Create Database Lock Table\nCREATE TABLE main.DATABASECHANGELOGLOCK (ID INT);\n"
    assert run(sql).returncode == 0
