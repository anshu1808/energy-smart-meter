import pytest
from bootstrap_environment import SCHEMAS, bootstrap_statements


def test_statements_are_idempotent_and_complete():
    stmts = bootstrap_statements("energy_dev")
    assert stmts[0] == "CREATE CATALOG IF NOT EXISTS energy_dev"
    assert all("IF NOT EXISTS" in s for s in stmts)
    assert len(stmts) == 1 + len(SCHEMAS)
    assert {"security", "liquibase", "audit"} <= set(SCHEMAS)


@pytest.mark.parametrize("bad", ["Energy", "energy-dev", "x; DROP CATALOG y", ""])
def test_invalid_catalog_rejected(bad):
    with pytest.raises(ValueError):
        bootstrap_statements(bad)
