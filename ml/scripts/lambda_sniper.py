#!/usr/bin/env python3
"""Poll Lambda Labs for GPU availability and launch as soon as capacity opens up.

Prints a catalog of all available instance types (with prices and regional
availability) before entering the polling loop, so you can immediately see
whether your targets are listed and what they cost.

Usage:
    python scripts/lambda_sniper.py [--instances TYPE [TYPE ...]]
                                   [--regions PREFIX [PREFIX ...]]
                                   [--poll-interval SECS] [--api-key KEY]
                                   [--dry-run]

Examples:
    python scripts/lambda_sniper.py
    python scripts/lambda_sniper.py --instances gpu_1x_a100_sxm4 --regions us
    python scripts/lambda_sniper.py --dry-run
"""

import argparse
import os
import time

import requests
from requests.auth import HTTPBasicAuth

API_BASE = "https://cloud.lambda.ai/api/v1"
DEFAULT_INSTANCES = ["gpu_1x_a100_sxm4", "gpu_1x_a100_pcie"]
DEFAULT_REGIONS = ["us", "eu"]


def auth(api_key):
    return HTTPBasicAuth(api_key, "")


def ts():
    return time.strftime("%H:%M:%S")


def elapsed(start: float) -> str:
    s = int(time.time() - start)
    h, m = divmod(s, 3600)
    m, s = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def get_ssh_key(api_key):
    resp = requests.get(f"{API_BASE}/ssh-keys", auth=auth(api_key), timeout=10)
    resp.raise_for_status()
    keys = resp.json().get("data", [])
    if not keys:
        raise RuntimeError("No SSH keys found in your Lambda account.")
    return keys[0]["name"]


def fetch_catalog(api_key):
    resp = requests.get(f"{API_BASE}/instance-types", auth=auth(api_key), timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", {})


def region_matches(name, prefixes):
    return any(name.startswith(p) for p in prefixes)


def print_catalog(data, targets, regions) -> int:
    target_set = set(targets)
    prefix_str = ", ".join(f"{p}*" for p in regions)

    rows = []
    for name, entry in sorted(data.items()):
        itype = entry.get("instance_type", {})
        desc = itype.get("description", "")
        cents = itype.get("price_cents_per_hour")
        price = f"${cents / 100:.2f}/hr" if cents is not None else "n/a"
        all_regions = [
            r["name"] for r in entry.get("regions_with_capacity_available", [])
        ]
        matching = [r for r in all_regions if region_matches(r, regions)]
        marker = "*" if name in target_set else " "
        rows.append((marker, name, desc, price, matching))

    name_w = max(len(r[1]) for r in rows)
    desc_w = max(len(r[2]) for r in rows)
    price_w = max(len(r[3]) for r in rows)

    print(
        f"\n  {'':1}  {'Instance':<{name_w}}  {'Description':<{desc_w}}  {'Price':>{price_w}}  Regions ({prefix_str})"
    )
    print(f"  {'':1}  {'-'*name_w}  {'-'*desc_w}  {'-'*price_w}  -------")
    for marker, name, desc, price, matching in rows:
        regions_str = ", ".join(matching) if matching else "—"
        print(
            f"  {marker}  {name:<{name_w}}  {desc:<{desc_w}}  {price:>{price_w}}  {regions_str}"
        )
    print()

    if not any(r[0] == "*" and r[4] for r in rows):
        print(
            f"  None of the target instances have capacity in {prefix_str} regions right now."
        )
    print()

    # 2 (blank line + header) + 1 (separator) + len(rows) + 1 (blank) + 1 (warning/blank) + 1 (trailing blank)
    return len(rows) + 6


def launch(api_key, itype, region, key_name):
    return requests.post(
        f"{API_BASE}/instance-operations/launch",
        json={
            "region_name": region,
            "instance_type_name": itype,
            "ssh_key_names": [key_name],
            "quantity": 1,
        },
        auth=auth(api_key),
        timeout=10,
    )


def snipe(api_key, instances, regions, poll_interval, dry_run):
    prefix_str = ", ".join(f"{p}*" for p in regions)

    print(f"[{ts()}] Fetching instance catalog...")
    try:
        data = fetch_catalog(api_key)
    except Exception as e:
        print(f"[{ts()}] Failed to fetch catalog: {e}")
        return

    catalog_lines = print_catalog(data, instances, regions)

    if dry_run:
        print(f"[{ts()}] DRY RUN — exiting without polling.")
        return

    try:
        key_name = get_ssh_key(api_key)
    except Exception as e:
        print(f"[{ts()}] {e}")
        return

    print(
        f"[{ts()}] Sniping {', '.join(instances)} in {prefix_str} — polling every {poll_interval}s"
    )

    checks = 0
    start = time.time()

    while True:
        try:
            data = fetch_catalog(api_key)
            checks += 1

            print(f"\033[{catalog_lines + 1}A\033[J", end="", flush=True)
            catalog_lines = print_catalog(data, instances, regions)

            for itype in instances:
                if itype not in data:
                    continue
                matching = [
                    r["name"]
                    for r in data[itype].get("regions_with_capacity_available", [])
                    if region_matches(r["name"], regions)
                ]
                if not matching:
                    continue

                region = matching[0]
                print(f"\n[{ts()}] AVAILABLE: {itype} in {matching}")
                print(f"[{ts()}] Launching {itype} in {region}...")
                resp = launch(api_key, itype, region, key_name)
                if resp.ok:
                    ids = resp.json().get("data", {}).get("instance_ids", [])
                    print(f"[{ts()}] Launched! Instance ID(s): {', '.join(ids)}")
                    print(
                        f"[{ts()}] Check status: https://cloud.lambdalabs.com/instances"
                    )
                else:
                    print(f"[{ts()}] Launch failed: {resp.text}")
                return

            print(
                f"[{ts()}] #{checks:04d}  {elapsed(start)} elapsed — no capacity",
                end="\r",
                flush=True,
            )
            time.sleep(poll_interval)

        except Exception as e:
            print(f"\n[{ts()}] Connection error: {e}")
            time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(
        description="Poll Lambda Labs for GPU availability and auto-launch.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("LAMBDA_API_KEY"),
        help="Lambda Labs API key. Set to `LAMBDA_API_KEY` by default.",
    )
    parser.add_argument(
        "--instances",
        nargs="+",
        default=DEFAULT_INSTANCES,
        metavar="TYPE",
        help=f"Instance type(s) to snipe (default: {' '.join(DEFAULT_INSTANCES)})",
    )
    parser.add_argument(
        "--regions",
        nargs="+",
        default=DEFAULT_REGIONS,
        metavar="PREFIX",
        help=f"Region prefix(es) to snipe in (default: {' '.join(DEFAULT_REGIONS)})",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=12.01,
        metavar="SECS",
        help="Seconds between polls (default: 12.01)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print catalog and exit without polling.",
    )
    args = parser.parse_args()

    if not args.api_key:
        raise ValueError(
            "No `LAMBDA_API_KEY` environment variable set. "
            "Either run with --api-key or define the environment variable."
        )

    try:
        snipe(
            args.api_key, args.instances, args.regions, args.poll_interval, args.dry_run
        )
    except KeyboardInterrupt:
        print(f"\n[{ts()}] Stopped.")


if __name__ == "__main__":
    main()
