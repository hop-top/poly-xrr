"""Subprocess calls to `az` are recorded/replayed by `xrr run`; no mocking."""
import json
import subprocess


def az(*args):
    return subprocess.run(["az", *args], capture_output=True, text=True)


def test_account_show():
    p = az("account", "show")
    assert p.returncode == 0
    assert json.loads(p.stdout)["name"] == "contoso"


def test_group_list():
    names = [g["name"] for g in json.loads(az("group", "list").stdout)]
    assert names == ["rg-app", "rg-data"]


def test_unknown_subcommand_exit_code():
    assert az("frobnicate").returncode == 2
