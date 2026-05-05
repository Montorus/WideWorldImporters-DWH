r"""Convert AdventureWorks SQL Server install CSV files for PostgreSQL import.

The Microsoft SQL Server install bundle uses a mix of regular tab-delimited
files and pipe-marked records (`+|` / `&|`). This script copies the source
folder to a target folder and converts only the files that need PostgreSQL
`\copy` compatible CSV formatting.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def csv_field(value: str) -> str:
    value = value.replace('"', '""')
    needs_quotes = (
        "\t" in value
        or "\n" in value
        or '"' in value
        or (value.startswith("<") and value.endswith(">"))
    )
    return f'"{value}"' if needs_quotes else value


def decode_candidate(raw: bytes, path: Path, is_address: bool) -> str | None:
    for encoding in ("utf-8", "cp1252", "utf-16", "utf-16le"):
        try:
            candidate = raw.decode(encoding)
        except UnicodeError:
            continue

        if (
            is_address
            or "+|" in candidate
            or "&|" in candidate
            or candidate.startswith("\ufeff")
        ):
            return candidate

    return None


def convert_file(path: Path) -> bool:
    raw = path.read_bytes()
    is_address = path.name == "Address.csv"
    text = decode_candidate(raw, path, is_address)

    if text is None:
        return False

    lines = text.splitlines(keepends=True)
    if not lines:
        return False

    if lines[0].startswith("\ufeff"):
        lines[0] = lines[0][1:]

    is_pipe_format = "+|" in lines[0]

    if is_pipe_format:
        output: list[str] = []
        pending = ""

        for line in lines:
            if line.strip().endswith("&|"):
                pending += (
                    line.replace("|474946383961", "|\\x474946383961")
                    .strip()[:-2]
                )
                output.append(
                    "\t".join(csv_field(part) for part in pending.split("+|")) + "\n"
                )
                pending = ""
            else:
                pending += line.replace("\r\n", "\\n").replace("\n", "\\n")

        path.write_text("".join(output), encoding="utf-8", newline="")
        return True

    if is_address or "&|" in text:
        output = []
        for line in lines:
            output.append(
                line.replace('"', '""')
                .replace("&|\n", "\n")
                .replace("&|\r\n", "\n")
                .replace("\tE6100000010C", "\t\\xE6100000010C")
                .replace("\r\n", "\n")
            )

        path.write_text("".join(output), encoding="utf-8", newline="")
        return True

    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_dir", type=Path)
    parser.add_argument("target_dir", type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    source_dir = args.source_dir.resolve()
    target_dir = args.target_dir.resolve()

    if not source_dir.is_dir():
        raise SystemExit(f"Source directory does not exist: {source_dir}")

    if target_dir.exists():
        if not args.force:
            raise SystemExit(f"Target already exists: {target_dir}. Use --force.")
        shutil.rmtree(target_dir)

    shutil.copytree(source_dir, target_dir)

    converted = []
    for csv_file in sorted(target_dir.glob("*.csv")):
        if convert_file(csv_file):
            converted.append(csv_file.name)

    print(f"Copied CSV directory: {source_dir} -> {target_dir}")
    print(f"Converted files: {len(converted)}")
    for name in converted:
        print(f"- {name}")


if __name__ == "__main__":
    main()
