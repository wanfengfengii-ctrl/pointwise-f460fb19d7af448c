import argparse
import json
import re
import unittest
from pathlib import Path
from .evidence import run_all


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    suite = unittest.defaultTestLoader.discover(str(root / "tests"))
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        raise SystemExit(1)
    if args.baseline:
        print(json.dumps(run_all(), ensure_ascii=False, sort_keys=True))
        return
    report = (root / "docs" / "online-compaction.md").read_text()
    evidence = json.loads((root / "docs" / "review-evidence.json").read_text())
    assert evidence["baseline"] == run_all(), "recorded baseline is not reproducible"
    required = {"publication", "checkpoint", "tombstones", "retry-receipt", "overlap"}
    assert {item["id"] for item in evidence["decisions"]} == required
    for item in evidence["decisions"]:
        assert item["choice"].strip() and item["code"]
        for location in item["code"]:
            match = re.fullmatch(r"(lake/[a-z_]+\.py):([1-9][0-9]*)", location)
            assert match, location
            source = root / match[1]
            assert 1 <= int(match[2]) <= len(source.read_text().splitlines()), location
            assert location in report, "citation missing from report: " + location
    print(json.dumps({"baseline": run_all(), "review": str(root / "docs" / "online-compaction.md"),
                      "decision_ids": sorted(required)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
