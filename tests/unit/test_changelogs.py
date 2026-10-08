"""Static checks on every Liquibase changeset: the policy a reviewer would otherwise check by eye."""

import pathlib
import re
import subprocess
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
CHANGELOGS = sorted((ROOT / "sql" / "ddl" / "changelogs").glob("*.yml"))
GUARD = str(ROOT / "ci" / "dbcr_guard.py")


def changesets():
    for f in CHANGELOGS:
        for item in yaml.safe_load(f.read_text(encoding="utf-8"))["databaseChangeLog"]:
            yield f, item["changeSet"]


def forward_sql(cs):
    return "\n".join(c["sql"]["sql"] for c in cs["changes"] if "sql" in c)


def test_changelogs_exist():
    assert CHANGELOGS, "no changesets found"


def test_every_changeset_follows_policy():
    for f, cs in changesets():
        assert cs["id"] == f.stem, f"{f.name}: id must equal filename"
        assert cs["runInTransaction"] is False, f"{f.name}: runInTransaction must be false"
        assert cs.get("rollback"), f"{f.name}: rollback required"
        assert cs.get("comment") and cs.get("labels") and cs.get("author"), f.name


def test_ids_unique_and_filenames_sorted():
    ids = [cs["id"] for _, cs in changesets()]
    assert len(ids) == len(set(ids))
    assert [f.name for f in CHANGELOGS] == sorted(f.name for f in CHANGELOGS)


def test_only_catalog_placeholder_and_no_hardcoded_catalog():
    for f in CHANGELOGS:
        text = f.read_text(encoding="utf-8")
        assert set(re.findall(r"\$\{(\w+)\}", text)) <= {"energy_catalog"}, f.name
        code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
        assert not re.search(r"\benergy\.(bronze|silver|gold|audit|security)\b", code), f"{f.name}: hardcoded catalog"


def test_forward_sql_passes_ddl_guard():
    """The CI guard blocks CREATE CATALOG/SCHEMA and holds DROP/DELETE/TRUNCATE in the forward plan."""
    for f, cs in changesets():
        sql = forward_sql(cs).replace("${energy_catalog}", "energy_dev")
        r = subprocess.run([sys.executable, GUARD], input=sql, text=True, capture_output=True, env={"PATH": ""})
        assert r.returncode == 0, f"{f.name} would be blocked in CI:\n{r.stderr}"


def test_dependency_order():
    order = [cs["id"] for _, cs in changesets()]
    pos = {i.split("-", 1)[1]: n for n, i in enumerate(order)}
    assert pos["create-security-mask-functions"] < pos["apply-column-masks"]
    assert pos["create-security-region-filter"] < pos["apply-region-row-filters"]
    assert pos["add-region-to-fact-consumption"] < pos["apply-region-row-filters"]
    assert pos["create-secure-revenue-view"] < pos["grants-business-user"]
    assert pos["apply-column-masks"] < pos["enable-iceberg-compat-dim-tariff"]


def test_masks_cover_expected_pii_columns():
    sql = next(forward_sql(cs) for _, cs in changesets() if cs["id"].endswith("apply-column-masks"))
    for col in ("customer_id", "LCLid", "bill_amount", "rate_per_kwh"):
        assert f"ALTER COLUMN {col} SET MASK" in sql


def test_properties_template_uses_parameter_prefix():
    """A bare `energy_catalog:` key is ignored by Liquibase 4.32; only `parameter.<name>` is substituted."""
    text = (ROOT / "sql" / "ddl" / "liquibase.properties.template").read_text(encoding="utf-8")
    assert re.search(r"^parameter\.energy_catalog:\s*\$\{CATALOG\}", text, re.M)
    assert not re.search(r"^energy_catalog:", text, re.M)
