#!/usr/bin/env python3
from __future__ import annotations

import sys
from collections import defaultdict
from openpyxl import load_workbook

def main(input_xlsx: str, output_path: str | None) -> int:
    wb = load_workbook(input_xlsx)
    if "Services" not in wb.sheetnames:
        print("Sheet 'Services' not found", file=sys.stderr)
        return 1

    ws = wb["Services"]
    headers = [str(c.value or "").strip() for c in ws[1]]
    try:
        host_i = headers.index("Host")
        unit_i = headers.index("Unit")
    except ValueError:
        print("Expected headers 'Host' and 'Unit' in Services sheet", file=sys.stderr)
        return 1

    by_host: dict[str, list[str]] = defaultdict(list)
    for row in ws.iter_rows(min_row=2, values_only=True):
        host = (row[host_i] or "").strip()
        unit = (row[unit_i] or "").strip()
        if host and unit:
            by_host[host].append(unit)

    out_lines: list[str] = []
    out_lines.append("# Commands generated from XLSX")
    out_lines.append("# 1) SSH into the host manually")
    out_lines.append("# 2) Copy/paste the block below")
    out_lines.append("")

    for host, units in sorted(by_host.items()):
        out_lines.append(f"echo '==== {host} ===='")
        out_lines.append("systemctl list-units --type=service --state=running")
        out_lines.append("")

        units = sorted(set(units))
        for unit in units:
            out_lines.append(f"echo '--- {unit} ---'")
            out_lines.append(f"systemctl show -p ExecStart -p FragmentPath -p EnvironmentFile {unit}")
            out_lines.append(f"systemctl status {unit} --no-pager || true")
            out_lines.append(f"journalctl -u {unit} -n 50 --no-pager || true")

            out_lines.append('UNIT_BASE="' + unit + '"')
            out_lines.append('UNIT_BASE="${UNIT_BASE%%.service*}"')

            out_lines.append('echo "Executable + potential app dirs"')
            out_lines.append('ls -ld /usr/bin/*"$UNIT_BASE"* /usr/local/bin/*"$UNIT_BASE"* 2>/dev/null || true')

            out_lines.append('echo "Config candidates"')
            out_lines.append('ls -ld /etc/*"$UNIT_BASE"* /etc/"$UNIT_BASE" 2>/dev/null || true')

            out_lines.append('echo "Data candidates"')
            out_lines.append('ls -ld /var/lib/*"$UNIT_BASE"* /var/lib/"$UNIT_BASE" /opt/*"$UNIT_BASE"* /opt/"$UNIT_BASE" 2>/dev/null || true')

            out_lines.append('echo "Log candidates"')
            out_lines.append('ls -ld /var/log/*"$UNIT_BASE"* /var/log/"$UNIT_BASE"* 2>/dev/null || true')

            out_lines.append("")

        out_lines.append('echo "Certificate candidates"')
        out_lines.append("find /etc -maxdepth 5 -type f 2>/dev/null | egrep -i '\\.(crt|pem|key|cer|pfx)$' | head")

        out_lines.append("")

    text = "\n".join(out_lines)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"[OK] wrote commands to {output_path}")
    else:
        sys.stdout.write(text)

    return 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_manual_commands.py <report.xlsx> [output.sh]", file=sys.stderr)
        raise SystemExit(2)
    input_xlsx = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) >= 3 else None
    raise SystemExit(main(input_xlsx, output_path))
