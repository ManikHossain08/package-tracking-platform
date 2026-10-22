"""Fail if any image, build tool, library or CI action is not pinned exactly (NFR-07, AC-0.6).

Standard library only, so it runs before any dependency is installed.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# Images built locally from a Dockerfile in this repo (whose FROM line is itself checked).
LOCAL_IMAGES = re.compile(r"ptp-[\w-]+:[\w.-]+")


def check_images(problems: list[str]) -> None:
    # One pattern per file type, so e.g. a Makefile target named `image:` is not mistaken for a Compose key.
    sources = [
        (ROOT / "infra" / "docker-compose.yml", r"^\s*image:\s*(\S+)"),
        (ROOT / "Makefile", r"_IMAGE\s*:?=\s*(\S+)"),
    ]
    sources += [(p, r"^FROM\s+(\S+)") for p in sorted((ROOT / "infra").glob("**/Dockerfile"))]
    for path, pattern in sources:
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            m = re.search(pattern, line)
            if m and LOCAL_IMAGES.fullmatch(m.group(1)):
                continue
            if m and not re.fullmatch(r"[\w./-]+:[\w.-]+@sha256:[0-9a-f]{64}", m.group(1)):
                rel = path.relative_to(ROOT)
                problems.append(f"{rel}:{lineno}: image not pinned by tag@digest: {m.group(1)}")


def check_sbt(problems: list[str]) -> None:
    props = (ROOT / "processor" / "project" / "build.properties").read_text()
    if not re.search(r"^sbt\.version=\d+\.\d+\.\d+$", props, re.M):
        problems.append("processor/project/build.properties: sbt.version must be an exact x.y.z")
    build = (ROOT / "processor" / "build.sbt").read_text()
    for lineno, line in enumerate(build.splitlines(), 1):
        # Library versions are declared once as `val <name>Version = "..."` and must be exact.
        m = re.search(r'(?:val \w+Version\s*=|scalaVersion\s*:=)\s*"([^"]*)"', line)
        if m and not re.fullmatch(r"\d+\.\d+\.\d+(\.\d+)?", m.group(1)):
            problems.append(f"processor/build.sbt:{lineno}: version is not exact: {m.group(1)!r}")
        # Dependencies must reference those vals, never an inline literal version.
        if re.search(r'%\s*"[^"]*"\s*%\s*"', line):
            problems.append(f"processor/build.sbt:{lineno}: inline dependency version; use a val")


def check_python(problems: list[str]) -> None:
    lock = (ROOT / "requirements-dev.txt").read_text()
    pkgs = re.findall(r"^([A-Za-z0-9_.\-\[\]]+)==\S+ \\\n(?:\s+--hash=sha256:[0-9a-f]{64}.*\n)+", lock, re.M)
    declared = re.findall(r"^([A-Za-z0-9_.\-\[\]]+)==", lock, re.M)
    if not declared or len(pkgs) != len(declared):
        problems.append("requirements-dev.txt: every package must be pinned with == and sha256 hashes")


def check_actions(problems: list[str]) -> None:
    for path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            m = re.search(r"uses:\s*(\S+)", line)
            if m and not re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", m.group(1)):
                rel = path.relative_to(ROOT)
                problems.append(f"{rel}:{lineno}: action not pinned to a commit SHA: {m.group(1)}")


def main() -> int:
    problems: list[str] = []
    for check in (check_images, check_sbt, check_python, check_actions):
        check(problems)
    for p in problems:
        print(f"UNPINNED {p}")
    print(f"check_pins: {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
