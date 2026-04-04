"""Validate SQLite database structural guarantees."""

import argparse
import os
import sqlite3
from functools import cache
from typing import Any

from src.utils import ALBUM, ARTIST, ENTITIES, LABEL, TABLES, Entity, Key, Table, entity_child


@cache
def get_entity_ids(conn: sqlite3.Connection, key: Key, table: Table) -> list[Any]:
    cursor = conn.cursor()
    keys = cursor.execute(f"SELECT {key} FROM {table}").fetchall()
    return keys


@cache
def get_searchable_ids(conn: sqlite3.Connection, key: Key, table: Table) -> list[Any]:
    cursor = conn.cursor()
    return cursor.execute(f"SELECT {key} FROM {table} WHERE searchable = 1").fetchall()


def table_id_set_inclusion(conn: sqlite3.Connection, entity: Entity, t1: Table, t2: Table) -> bool:
    t1_ids = get_entity_ids(conn, entity.key, t1)
    t2_ids = get_entity_ids(conn, entity.key, t2)
    return set(t1_ids).issubset(t2_ids)


def main():
    parser = argparse.ArgumentParser(
        description="Validate SQLite database structural guarantees.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("SICK_DB"),
        metavar="PATH",
        help="Path to sick.db. $SICK_DB",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Disable ANSI escape codes in output.",
    )
    args = parser.parse_args()

    if args.db is None:
        raise ValueError("--db / $SICK_DB not set")

    BOLD = "" if args.raw else "\033[1m"
    GREEN = "" if args.raw else "\033[92m"
    RED = "" if args.raw else "\033[91m"
    RESET = "" if args.raw else "\033[0m"
    DIM = "" if args.raw else "\033[2m"

    print(f"{BOLD}sick db check{RESET}")
    print(f"{DIM}{args.db}{RESET}\n")

    failed = False

    def report(label: str, ok: bool) -> None:
        nonlocal failed
        if ok:
            print(f"  {label}  {GREEN}{BOLD}ok{RESET}")
        else:
            print(f"  {label}  {RED}{BOLD}failed{RESET}")
            failed = True

    def check(conn: sqlite3.Connection, entity: Entity, t1: Table, t2: Table) -> None:
        ok = table_id_set_inclusion(conn, entity, t1, t2)
        report(f"{t1}.{entity.key} ⊆ {t2}.{entity.key}", ok)

    with sqlite3.connect(args.db) as conn:
        print("table inclusions")
        for entity in ENTITIES:
            for child in entity_child(entity):
                check(conn, child, entity.table, child.table)
                check(conn, child, child.table, entity.table)

        print("repr inclusions")
        for entity, rpr in [
            (ALBUM, TABLES.album_repr_tracks),
            (ARTIST, TABLES.artist_repr_albums),
            (LABEL, TABLES.label_repr_artists),
        ]:
            check(conn, entity, rpr, entity.table)
            # it's expected for these to fail, as only searchable entities can
            # end up in a repr table.
            check(conn, entity, entity.table, rpr)

        print("searchable repr coverage")
        for entity in ENTITIES:
            if entity.repr is None:
                continue
            searchable_ids = get_searchable_ids(conn, entity.key, entity.table)
            repr_ids = get_entity_ids(conn, entity.key, entity.repr)
            ok = set(searchable_ids).issubset(repr_ids)
            report(f"{entity.table}[searchable].{entity.key} ⊆ {entity.repr}.{entity.key}", ok)

        print("embedding inclusions")
        for entity in ENTITIES:
            check(conn, entity, entity.embedding, entity.table)
            check(conn, entity, entity.table, entity.embedding)

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
