"""Validate SQLite database structural guarantees."""

import argparse
from functools import cache
import os
import sqlite3
from typing import Any

from src.utils import ALBUM
from src.utils import ARTIST
from src.utils import ENTITIES
from src.utils import Entity
from src.utils import entity_child
from src.utils import LABEL
from src.utils import TRACK


def has_no_blank(conn: sqlite3.Connection, key: str, table: str) -> bool:
    return conn.execute(
        f"SELECT 1 FROM {table} WHERE {key} = '' LIMIT 1"
    ).fetchone() is None


@cache
def get_entity_ids(conn: sqlite3.Connection, key: str, table: str) -> list[Any]:
    cursor = conn.cursor()
    keys = cursor.execute(f"SELECT {key} FROM {table}").fetchall()
    return keys


@cache
def get_searchable_ids(conn: sqlite3.Connection, key: str, table: str) -> list[Any]:
    cursor = conn.cursor()
    return cursor.execute(f"SELECT {key} FROM {table} WHERE searchable = 1").fetchall()


def table_id_set_inclusion(conn: sqlite3.Connection, entity: Entity, t1: str, t2: str) -> bool:
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
    RED = "" if args.raw else "\033[91m"
    GREEN = "" if args.raw else "\033[92m"
    YELLOW = "" if args.raw else "\033[93m"
    RESET = "" if args.raw else "\033[0m"
    DIM = "" if args.raw else "\033[2m"

    print(f"{BOLD}sick db check{RESET}")
    print(f"{DIM}{args.db}{RESET}\n")

    failed = False

    def report(label: str, ok: bool, acceptable: bool = False) -> None:
        nonlocal failed
        if ok:
            print(f"  {label}  {GREEN}{BOLD}true{RESET}")
            return
        # acceptable means this false result is expected for current data semantics.
        if acceptable:
            print(f"  {label}  {YELLOW}{BOLD}false{RESET}")
            return
        print(f"  {label}  {RED}{BOLD}false{RESET}")
        failed = True

    with sqlite3.connect(args.db) as conn:
        print("blank names")
        for key, table in (
            ("track_name", TRACK.table),
            ("track_name_norm", TRACK.table),
            ("album_name", TRACK.table),
            ("artist_name", TRACK.table),
            ("label", TRACK.table),
            ("album_name", ALBUM.table),
            ("album_name_norm", ALBUM.table),
            ("artist_name", ALBUM.table),
            ("label", ALBUM.table),
            ("artist_name", ARTIST.table),
            ("label", LABEL.table),
        ):
            # there are tracks with empty names and we choose to respect them
            acceptable = table == TRACK.table and key in {"track_name", "track_name_norm"}
            report(f"no blanks in {table}.{key}", has_no_blank(conn, key, table), acceptable)

        print("table inclusions")
        for entity in ENTITIES:
            for child in entity_child(entity):
                for t1, t2 in ((entity.table, child.table), (child.table, entity.table)):
                    ok = table_id_set_inclusion(conn, child, t1, t2)
                    # we have extra artist entries from features
                    acceptable = child == ARTIST and t1 == ARTIST.table and t2 == ALBUM.table
                    report(f"{t1}.{child.key} ⊆ {t2}.{child.key}", ok, acceptable)

        print("repr inclusions")
        for entity in (ALBUM, ARTIST, LABEL):
            rpr = entity.repr
            assert rpr is not None
            ok = table_id_set_inclusion(conn, entity, rpr, entity.table)
            report(f"{rpr}.{entity.key} ⊆ {entity.table}.{entity.key}", ok)

        print("searchable repr coverage")
        for entity in ENTITIES:
            if entity == TRACK:
                continue
            searchable_ids = get_searchable_ids(conn, entity.repr_entity.key, entity.repr_entity.table)
            repr_ids = get_entity_ids(conn, entity.repr_entity.key, entity.repr)
            ok = set(repr_ids).issubset(searchable_ids)
            report(
                f"{entity.repr}.{entity.repr_entity.key} ⊆ {entity.repr_entity.table}[searchable].{entity.repr_entity.key}",
                ok,
            )

        print("nrepr consistency")
        for entity in (ALBUM, ARTIST, LABEL):
            rpr = entity.repr
            assert rpr is not None
            mismatches = conn.execute(
                f"SELECT COUNT(*) FROM {entity.table} e "
                f"WHERE e.nrepr != (SELECT COUNT(*) FROM {rpr} r "
                f"WHERE r.{entity.key} = e.{entity.key})"
            ).fetchone()[0]
            ok = mismatches == 0
            report(f"{entity.table}.nrepr matches {rpr} counts", ok)

        print("embedding inclusions")
        for entity in ENTITIES:
            for t1, t2 in ((entity.embedding, entity.table), (entity.table, entity.embedding)):
                ok = table_id_set_inclusion(conn, entity, t1, t2)
                report(f"{t1}.{entity.key} ⊆ {t2}.{entity.key}", ok)

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
