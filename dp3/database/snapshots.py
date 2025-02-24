import abc
import logging
import re
from abc import ABC
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from ipaddress import IPv4Address, IPv6Address
from typing import Any, Optional, Union

import pymongo
from bson import Binary
from pymongo import UpdateMany, UpdateOne
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.errors import BulkWriteError, DocumentTooLarge, OperationFailure, WriteError

from dp3.common.attrspec import AttrSpecType, AttrType
from dp3.common.config import ModelSpec
from dp3.common.datatype import AnyEidT
from dp3.common.mac_address import MACAddress
from dp3.common.utils import int2bytes
from dp3.database.config import MongoConfig
from dp3.database.encodings import BSON_OBJECT_TOO_LARGE, get_codec_options
from dp3.database.exceptions import SnapshotCollectionError
from dp3.database.magic import search_and_replace


class TypedSnapshotCollection(abc.ABC):
    """Snapshot collection handler with eid type awareness."""

    def __init__(
        self,
        db: Database,
        entity_type: str,
        db_config: MongoConfig,
        model_spec: ModelSpec,
        snapshots_config: dict,
    ):
        self._db = db.with_options(codec_options=get_codec_options())

        if entity_type not in model_spec.entities:
            raise ValueError(f"Entity type '{entity_type}' not found in model spec")
        self.entity_type = entity_type
        self._col_name = f"{entity_type}#snapshots"
        self._os_col_name = f"{entity_type}#oversized_snapshots"

        self.attr_specs: dict[str, AttrSpecType] = model_spec.entity_attributes[entity_type]

        self.log = logging.getLogger(f"EntityDatabase.SnapshotCollection[{entity_type}]")

        self._normal_snapshot_eids = set()
        self._oversized_snapshot_eids = set()
        self._snapshot_bucket_size = db_config.storage.snapshot_bucket_size
        self._bucket_delta = self._get_snapshot_bucket_delta(snapshots_config)

    def _get_snapshot_bucket_delta(self, config) -> timedelta:
        """Returns how long it takes to fill a snapshot bucket.

        This depends on the frequency of snapshot creation and the bucket size.
        """
        creation_rate = config.get("creation_rate", {"minute": "*/30"})
        if len(creation_rate) > 1:
            raise ValueError("Only one snapshot creation rate is supported.")

        time_to_sec = {
            "second": 1,
            "minute": 60,
            "hour": 60 * 60,
            "day": 60 * 60 * 24,
        }

        time_to_sec_shifted = {
            "second": 60,
            "minute": 60 * 60,
            "hour": 60 * 60 * 24,
            "day": 60 * 60 * 24 * 30,
        }

        for key, value in creation_rate.items():
            if value.startswith("*/"):
                seconds = time_to_sec.get(key)
                if seconds is None:
                    raise ValueError(f"Unsupported snapshot creation rate: {creation_rate}")

                snapshot_interval = int(value[2:])
                bucket_delta = seconds * snapshot_interval
            else:
                seconds = time_to_sec_shifted.get(key)
                if seconds is None:
                    raise ValueError(f"Unsupported snapshot creation rate: {creation_rate}")

                count = len(value.split(","))
                bucket_delta = seconds // count
            break
        else:
            raise ValueError(f"Unsupported snapshot creation rate: {creation_rate}")

        return timedelta(seconds=bucket_delta * self._snapshot_bucket_size)

    def _col(self, **kwargs) -> Collection:
        """Returns entity snapshots collection.

        **kwargs: additional arguments for `pymongo.Database.get_collection`.
        """
        return self._db.get_collection(self._col_name, **kwargs)

    def _os_col(self, **kwargs) -> Collection:
        """Returns entity snapshots collection.

        **kwargs: additional arguments for `pymongo.Database.get_collection`.
        """
        return self._db.get_collection(self._os_col_name, **kwargs)

    @staticmethod
    def _pack_binary_bucket_id(eid: bytes, ts_int: int) -> Binary:
        return Binary(b"".join([eid, ts_int.to_bytes(8, "big", signed=True)]))

    def _binary_bucket_range(self, eid: bytes) -> dict:
        return {
            "$gte": self._pack_binary_bucket_id(eid, 0),
            "$lt": self._pack_binary_bucket_id(eid, -1),
        }

    @abc.abstractmethod
    def _bucket_id(self, eid: AnyEidT, ctime: datetime) -> Union[str, Binary]:
        """Returns `_id` for snapshot bucket document."""

    @abc.abstractmethod
    def _filter_from_bid(self, b_id: Union[bytes, str]) -> dict:
        """Returns filter for snapshots with same eid as given bucket document _id.
        Args:
            b_id: the _id of the snapshot bucket, type depends on etype's data type
        """

    @abc.abstractmethod
    def _filter_from_eid(self, eid: AnyEidT) -> dict:
        """Returns filter for snapshots with given eid."""

    @abc.abstractmethod
    def _filter_from_eids(self, eids: Iterable[Any]) -> dict:
        """Returns filter for snapshots with given eids."""

    def get_latest_one(self, eid: AnyEidT) -> dict:
        """Get latest snapshot of given eid.

        If doesn't exist, returns {}.
        """
        return (
            self._col().find_one(self._filter_from_eid(eid), {"last": 1}, sort=[("_id", -1)]) or {}
        ).get("last", {})

    def get_latest(
        self,
        fulltext_filters: Optional[dict[str, str]] = None,
        generic_filter: Optional[dict[str, Any]] = None,
    ) -> tuple[Cursor, int]:
        """Get latest snapshots of given `etype`.

        This method is useful for displaying data on web.

        Returns only documents matching `generic_filter` and `fulltext_filters`
        (dictionary attribute - fulltext filter).
        Fulltext filters are interpreted as regular expressions.
        Only string values may be filtered this way. There's no validation that queried attribute
        can be fulltext filtered.
        Only plain and observation attributes with string-based data types can be queried.
        Array and set data types are supported as well as long as they are not multi value
        at the same time.
        If you need to filter EIDs, ensure the EID is string, then use attribute `eid`.
        Otherwise, use generic filter.

        Generic filter allows filtering using generic MongoDB query (including `$and`, `$or`,
        `$lt`, etc.).
        For querying non-JSON-native types, you can use magic strings, such as
        `"$$IPv4{<ip address>}"` for IPv4 addresses. The full spec with examples is in the
        [magic strings module][dp3.database.magic].

        Generic and fulltext filters are merged - fulltext overrides conflicting keys.

        Also returns total document count (after filtering).

        May raise `SnapshotCollectionError` if query is invalid.
        """
        snapshot_col = self._col()
        query = self._prepare_latest_query(fulltext_filters or {}, generic_filter or {})

        try:
            return snapshot_col.find(query, {"last": 1}).sort(
                [("_id", pymongo.ASCENDING)]
            ), snapshot_col.count_documents(query)
        except OperationFailure as e:
            raise SnapshotCollectionError(f"Query is invalid: {e}") from e

    def find_latest(
        self,
        fulltext_filters: Optional[dict[str, str]] = None,
        generic_filter: Optional[dict[str, Any]] = None,
    ) -> Cursor:
        """Find latest snapshots of given `etype`.

        See [`get_latest`][dp3.database.snapshots.SnapshotCollectionContainer.get_latest]
        for more information.

        Returns only documents matching `generic_filter` and `fulltext_filters`,
        does not count them.
        """
        query = self._prepare_latest_query(fulltext_filters or {}, generic_filter or {})
        try:
            return self._col().find(query, {"last": 1}).sort([("_id", pymongo.ASCENDING)])
        except OperationFailure as e:
            raise SnapshotCollectionError(f"Query is invalid: {e}") from e

    def count_latest(
        self,
        fulltext_filters: Optional[dict[str, str]] = None,
        generic_filter: Optional[dict[str, Any]] = None,
    ) -> int:
        """Count latest snapshots of given `etype`.

        See [`get_latest`][dp3.database.snapshots.SnapshotCollectionContainer.get_latest]
        for more information.

        Returns only count of documents matching `generic_filter` and `fulltext_filters`.

        Note that this method may take much longer than `get_latest` on larger databases,
        as it does count all documents, not just return the first few.
        """
        query = self._prepare_latest_query(fulltext_filters or {}, generic_filter or {})
        try:
            return self._col().count_documents(query)
        except OperationFailure as e:
            raise SnapshotCollectionError(f"Query is invalid: {e}") from e

    def _prepare_latest_query(
        self, fulltext_filters: dict[str, str], generic_filter: dict[str, Any]
    ):
        """Prepare query for get_latest method."""
        # Create base of query
        try:
            query = search_and_replace(generic_filter)
        except ValueError as e:
            raise SnapshotCollectionError(f"Invalid generic filter: {str(e)}") from e
        query["latest"] = True

        # Process fulltext filters
        for attr, attr_filter in fulltext_filters.items():
            fulltext_filter = {"$regex": attr_filter, "$options": "i"}

            # EID filter
            if attr == "eid":
                query["_id"] = fulltext_filter
                continue

            # Check if attribute exists
            try:
                attr_spec = self.attr_specs[attr]
            except KeyError as e:
                raise SnapshotCollectionError(
                    f"Attribute '{attr}' in fulltext filter doesn't exist"
                ) from e

            # Correctly handle link<...> data type
            if attr_spec.t in AttrType.PLAIN | AttrType.OBSERVATIONS and attr_spec.is_relation:
                query["last." + attr + ".eid"] = fulltext_filter
            else:
                query["last." + attr] = fulltext_filter

        return query

    def get_by_eid(
        self, eid: AnyEidT, t1: Optional[datetime] = None, t2: Optional[datetime] = None
    ) -> Union[Cursor, CommandCursor]:
        """Get all (or filtered) snapshots of given `eid`.

        This method is useful for displaying `eid`'s history on web.

        Args:
            eid: id of entity, to which data-points correspond
            t1: left value of time interval (inclusive)
            t2: right value of time interval (inclusive)
        """
        snapshot_col = self._col()

        # Find out if the snapshot is oversized
        doc = (
            snapshot_col.find(self._filter_from_eid(eid), {"oversized": 1})
            .sort([("_id", -1)])
            .limit(1)
        )
        doc = next(doc, None)
        if doc and doc.get("oversized", False):
            return self._get_oversized(eid, t1, t2)

        query = {"_time_created": {}}
        pipeline = [
            {"$match": self._filter_from_eid(eid)},
            {"$unwind": "$history"},
            {"$replaceRoot": {"newRoot": "$history"}},
        ]

        # Filter by date
        if t1:
            query["_time_created"]["$gte"] = t1
        if t2:
            query["_time_created"]["$lte"] = t2

        # Unset if empty
        if query["_time_created"]:
            pipeline.append({"$match": query})
        pipeline.append({"$sort": {"_time_created": pymongo.ASCENDING}})
        return snapshot_col.aggregate(pipeline)

    def get_distinct_val_count(self, attr: str) -> dict[Any, int]:
        """Counts occurrences of distinct values of given attribute in snapshots.

        Returns dictionary mapping value -> count.

        Works for all plain and observation data types except `dict` and `json`.
        """
        # Get attribute specification
        try:
            attr_spec = self.attr_specs[attr]
        except KeyError as e:
            raise SnapshotCollectionError(f"Attribute '{attr}' does not exist") from e

        if attr_spec.t not in AttrType.PLAIN | AttrType.OBSERVATIONS:
            raise SnapshotCollectionError(f"Attribute '{attr}' isn't plain or observations")

        # Attribute data type must be primitive, array<T> or set<T>
        if any(needle in attr_spec.data_type.root for needle in ("dict", "json")):
            raise SnapshotCollectionError(
                f"Data type '{attr_spec.data_type}' of attribute '{attr}' is not processable"
            )

        # Build aggregation query
        attr_path = "$last." + attr
        unwinding = []

        # Unwind array-like and multi value attributes
        # If attribute is multi value array, unwind twice
        if "array" in attr_spec.data_type.root or "set" in attr_spec.data_type.root:
            unwinding.append({"$unwind": attr_path})
        if attr_spec.t == AttrType.OBSERVATIONS and attr_spec.multi_value:
            unwinding.append({"$unwind": attr_path})

        # Group
        agg_query_group_id = attr_path
        if "link" in attr_spec.data_type.root:
            agg_query_group_id += ".eid"

        agg_query = [
            {"$match": {"latest": True}},
            *unwinding,
            {"$group": {"_id": agg_query_group_id, "count": {"$sum": 1}}},
            {"$sort": {"_id": 1, "count": -1}},
        ]
        # Run aggregation
        distinct_counts_cur = self._col().aggregate(agg_query)

        distinct_counts = {x["_id"]: x["count"] for x in distinct_counts_cur}

        if None in distinct_counts:
            del distinct_counts[None]

        return distinct_counts

    def _get_oversized(
        self, eid: AnyEidT, t1: Optional[datetime] = None, t2: Optional[datetime] = None
    ) -> Cursor:
        """Get all (or filtered) snapshots of given `eid` from oversized snapshots collection."""
        snapshot_col = self._os_col()
        query = {"eid": eid, "_time_created": {}}

        # Filter by date
        if t1:
            query["_time_created"]["$gte"] = t1
        if t2:
            query["_time_created"]["$lte"] = t2

        # Unset if empty
        if not query["_time_created"]:
            del query["_time_created"]

        return snapshot_col.find(query, projection={"_id": False}).sort(
            [("_time_created", pymongo.ASCENDING)]
        )

    def _migrate_to_oversized(self, eid: AnyEidT, snapshot: dict):
        snapshot_col = self._col()
        os_col = self._os_col()

        try:
            move_to_oversized = []
            last_id = None
            for doc in snapshot_col.find(self._filter_from_eid(eid)).sort({"_id": 1}):
                move_to_oversized.extend(doc.get("history", []))
                last_id = doc["_id"]
            move_to_oversized.insert(0, snapshot)

            os_col.insert_many(move_to_oversized)
            snapshot_col.update_one(
                {"_id": last_id},
                {
                    "$set": {"oversized": True, "last": snapshot, "count": 0},
                    "$unset": {"history": ""},
                },
            )
            snapshot_col.delete_many(self._filter_from_eid(eid) | {"oversized": False})
        except Exception as e:
            raise SnapshotCollectionError(
                f"Update of snapshot {eid} failed: {e}, {snapshot}"
            ) from e

    def save_one(self, snapshot: dict, ctime: datetime):
        """Saves snapshot to specified entity of current master document.

        Will move snapshot to oversized snapshots if the maintained bucket is too large.
        """
        if "eid" not in snapshot:
            self.log.error("Snapshot is missing 'eid' field: %s", snapshot)
            return
        eid = snapshot["eid"]
        snapshot["_time_created"] = ctime

        snapshot_col = self._col()
        os_col = self._os_col()

        # Find out if the snapshot is oversized
        normal, oversized = self._get_state({eid})
        if normal:
            try:
                res = snapshot_col.update_one(
                    self._filter_from_eid(eid) | {"count": {"$lt": self._snapshot_bucket_size}},
                    {
                        "$set": {"last": snapshot},
                        "$push": {"history": {"$each": [snapshot], "$position": 0}},
                        "$inc": {"count": 1},
                        "$setOnInsert": {
                            "_id": self._bucket_id(eid, ctime),
                            "_time_created": ctime,
                            "oversized": False,
                            "latest": True,
                        },
                    },
                    upsert=True,
                )

                if res.upserted_id is not None:
                    snapshot_col.update_many(
                        self._filter_from_eid(eid)
                        | {"latest": True, "count": self._snapshot_bucket_size},
                        {"$unset": {"latest": 1}},
                    )
            except (WriteError, OperationFailure, DocumentTooLarge) as e:
                if e.code != BSON_OBJECT_TOO_LARGE:
                    raise e
                # The snapshot is too large, move it to oversized snapshots
                self.log.info(f"Snapshot of {eid} is too large: {e}, marking as oversized.")
                self._migrate_to_oversized(eid, snapshot)
                self._cache_snapshot_state(set(), normal)
            except Exception as e:
                raise SnapshotCollectionError(
                    f"Insert of snapshot {eid} failed: {e}, {snapshot}"
                ) from e
            return
        elif oversized:
            # Snapshot is already marked as oversized
            snapshot_col.update_one(self._filter_from_eid(eid), {"$set": {"last": snapshot}})
            os_col.insert_one(snapshot)
            return

    def _get_state(self, eids: set[str]) -> tuple[set, set]:
        """Get current state of snapshot of given `eid`."""
        unknown = eids
        normal = self._normal_snapshot_eids & unknown
        oversized = self._oversized_snapshot_eids & unknown
        unknown = unknown - normal - oversized

        if not unknown:
            return normal, oversized

        snapshot_col = self._col()
        new_oversized = set()
        for doc in snapshot_col.find(
            self._filter_from_eids(unknown) | {"oversized": True},
            {"oversized": 1},
        ):
            eid = doc["_id"].rsplit("_#", maxsplit=1)[0]
            new_oversized.add(eid)

        unknown = unknown - new_oversized
        self._normal_snapshot_eids |= unknown
        self._oversized_snapshot_eids |= new_oversized

        return normal | unknown, oversized | new_oversized

    def _cache_snapshot_state(self, normal: set, oversized: set):
        """Cache snapshot state for given `etype`."""
        self._normal_snapshot_eids |= normal

        self._normal_snapshot_eids -= oversized
        self._oversized_snapshot_eids |= oversized

    def save_many(self, snapshots: list[dict], ctime: datetime):
        """
        Saves a list of snapshots of current master documents.

        All snapshots must belong to same entity type.

        Will move snapshots to oversized snapshots if the maintained bucket is too large.
        For better understanding, see `save()`.
        """

        for snapshot in snapshots:
            snapshot["_time_created"] = ctime

        snapshot_col = self._col()
        os_col = self._os_col()

        snapshots_by_eid = defaultdict(list)
        for snapshot in snapshots:
            if "eid" not in snapshot:
                continue
            if "_id" in snapshot:
                del snapshot["_id"]
            snapshots_by_eid[snapshot["eid"]].append(snapshot)

        # Find out if any of the snapshots are oversized
        normal, oversized = self._get_state(set(snapshots_by_eid.keys()))

        upserts = []
        update_originals: list[list[dict]] = []
        oversized_inserts = []
        oversized_updates = []

        # A normal snapshot, shift the last snapshot to history and update last
        for eid in normal:
            upserts.append(
                UpdateOne(
                    self._filter_from_eid(eid) | {"count": {"$lt": self._snapshot_bucket_size}},
                    {
                        "$set": {"last": snapshots_by_eid[eid][-1]},
                        "$push": {"history": {"$each": snapshots_by_eid[eid], "$position": 0}},
                        "$inc": {"count": len(snapshots_by_eid[eid])},
                        "$setOnInsert": {
                            "_id": self._bucket_id(eid, ctime),
                            "_time_created": ctime,
                            "oversized": False,
                            "latest": True,
                        },
                    },
                    upsert=True,
                )
            )
            update_originals.append(snapshots_by_eid[eid])

        # Snapshot is already marked as oversized
        for eid in oversized:
            oversized_inserts.extend(snapshots_by_eid[eid])
            oversized_updates.append(
                UpdateOne(
                    self._filter_from_eid(eid),
                    {"$set": {"last": snapshots_by_eid[eid][-1]}},
                )
            )

        new_oversized = set()

        if upserts:
            try:
                res = snapshot_col.bulk_write(upserts, ordered=False)

                # Unset latest snapshots if new snapshots were inserted
                if res.upserted_count > 0:
                    unset_latest_updates = []
                    for upsert_id in res.upserted_ids.values():
                        unset_latest_updates.append(
                            UpdateMany(
                                self._filter_from_bid(upsert_id)
                                | {"latest": True, "count": self._snapshot_bucket_size},
                                {"$unset": {"latest": 1}},
                            )
                        )
                    up_res = snapshot_col.bulk_write(unset_latest_updates)
                    if up_res.modified_count != res.upserted_count:
                        self.log.info(
                            "Upserted the first snapshot for %d entities.",
                            res.upserted_count - up_res.modified_count,
                        )

                if res.modified_count + res.upserted_count != len(upserts):
                    self.log.error(
                        "Some snapshots were not updated, %s != %s",
                        res.modified_count + res.upserted_count,
                        len(upserts),
                    )
            except (BulkWriteError, OperationFailure) as e:
                self.log.info("Update of snapshots failed, will retry with oversize.")
                failed_indexes = [
                    err["index"]
                    for err in e.details["writeErrors"]
                    if err["code"] == BSON_OBJECT_TOO_LARGE
                ]
                failed_snapshots = (update_originals[i] for i in failed_indexes)
                for eid_snapshots in failed_snapshots:
                    eid = eid_snapshots[0]["eid"]
                    failed_snapshots = sorted(
                        eid_snapshots, key=lambda s: s["_time_created"], reverse=True
                    )
                    self._migrate_to_oversized(eid, failed_snapshots[0])
                    oversized_inserts.extend(failed_snapshots[1:])
                    new_oversized.add(eid)

                if any(err["code"] != BSON_OBJECT_TOO_LARGE for err in e.details["writeErrors"]):
                    # Some other error occurred
                    raise e
            except Exception as e:
                raise SnapshotCollectionError(f"Upsert of snapshots failed: {str(e)[:2048]}") from e

        # Update the oversized snapshots
        if oversized_inserts:
            try:
                if oversized_updates:
                    snapshot_col.bulk_write(oversized_updates)
                os_col.insert_many(oversized_inserts)
            except Exception as e:
                raise SnapshotCollectionError(f"Insert of snapshots failed: {str(e)[:2048]}") from e

        # Cache the new state
        self._cache_snapshot_state(set(), new_oversized)

    def delete_old(self, t_old: datetime) -> int:
        """Delete old snapshots.

        Periodically called from HistoryManager.
        """
        deleted = 0
        try:
            res = self._col().delete_many(
                {"_time_created": {"$lte": t_old - self._bucket_delta}},
            )
            deleted += res.deleted_count * self._snapshot_bucket_size
            res = self._os_col().delete_many({"_time_created": {"$lt": t_old}})
            deleted += res.deleted_count
        except Exception as e:
            raise SnapshotCollectionError(f"Delete of olds snapshots failed: {e}") from e
        return deleted

    def delete_eid(self, eid: AnyEidT):
        """Delete all snapshots of `eid`."""
        try:
            res = self._col().delete_many(self._filter_from_eid(eid))
            del_cnt = res.deleted_count * self._snapshot_bucket_size
            self.log.debug("deleted %s snapshots of %s/%s.", del_cnt, self.entity_type, eid)

            res = self._os_col().delete_many({"eid": eid})
            self.log.debug(
                "Deleted %s oversized snapshots of %s/%s.", res.deleted_count, self.entity_type, eid
            )
        except Exception as e:
            raise SnapshotCollectionError(f"Delete of failed: {e}\n{eid}") from e

    def delete_eids(self, eids: list[Any]):
        """Delete all snapshots of `eids`."""
        try:
            res = self._col().delete_many(self._filter_from_eids(eids))
            del_cnt = res.deleted_count * self._snapshot_bucket_size
            self.log.debug("Deleted %s snapshots of %s (%s).", del_cnt, self.entity_type, len(eids))
            res = self._os_col().delete_many({"eid": {"$in": eids}})
            self.log.debug(
                "Deleted %s oversized snapshots of %s (%s).",
                res.deleted_count,
                self.entity_type,
                len(eids),
            )
        except Exception as e:
            raise SnapshotCollectionError(f"Delete of snapshots failed: {e}\n{eids}") from e


class StringEidSnapshots(TypedSnapshotCollection):
    def _bucket_id(self, eid: str, ctime: datetime) -> str:
        return f"{eid}_#{int(ctime.timestamp())}"

    def _filter_from_eid(self, eid: str) -> dict:
        return {"_id": {"$regex": f"^{re.escape(str(eid))}_#"}}

    def _filter_from_eids(self, eids: Iterable[str]) -> dict:
        return {"_id": {"$regex": "|".join([f"^{re.escape(eid)}_#" for eid in eids])}}

    def _filter_from_bid(self, b_id: str) -> dict:
        eid = b_id.rsplit("_#", maxsplit=1)[0]
        return {"_id": {"$regex": f"^{re.escape(eid)}_#"}}


class BinaryEidSnapshots(TypedSnapshotCollection, ABC):
    @abc.abstractmethod
    def _get_packed(self, eid: AnyEidT) -> bytes:
        """Return the packed bytes representation of the given eid."""

    def _filter_from_bid(self, b_id: bytes) -> dict:
        eid_bytes = b_id[:-8]
        return {"_id": self._binary_bucket_range(eid_bytes)}

    def _bucket_id(self, eid: AnyEidT, ctime: datetime) -> Binary:
        ts = int(ctime.timestamp())
        return self._pack_binary_bucket_id(self._get_packed(eid), ts)

    def _filter_from_eid(self, eid: AnyEidT) -> dict:
        return {"_id": self._binary_bucket_range(self._get_packed(eid))}

    def _filter_from_eids(self, eids: Iterable[Any]) -> dict:
        return {"$or": [self._filter_from_eid(eid) for eid in eids]}


class IntEidSnapshots(BinaryEidSnapshots):
    def _get_packed(self, eid: int) -> bytes:
        return int2bytes(eid)


class IPv4EidSnapshots(BinaryEidSnapshots):
    def _get_packed(self, eid: IPv4Address) -> bytes:
        return eid.packed


class IPv6EidSnapshots(BinaryEidSnapshots):
    def _get_packed(self, eid: IPv6Address) -> bytes:
        return eid.packed


class MACAddressEidSnapshots(BinaryEidSnapshots):
    def _get_packed(self, eid: MACAddress) -> bytes:
        return eid.packed


entity_type2collection = {
    "string": StringEidSnapshots,
    "int": IntEidSnapshots,
    "ipv4": IPv4EidSnapshots,
    "ipv6": IPv6EidSnapshots,
    "mac": MACAddressEidSnapshots,
}


class SnapshotCollectionContainer:
    """Container for all required snapshot collections, exposing the public interface."""

    def __init__(
        self, db: Database, db_config: MongoConfig, model_spec: ModelSpec, snapshots_config: dict
    ):
        self._snapshot_collections = {}
        for entity_type, entity_spec in model_spec.entities.items():
            eid_type_name = entity_spec.id_data_type.root
            typed_collection: type[TypedSnapshotCollection] = entity_type2collection[eid_type_name]
            self._snapshot_collections[entity_type] = typed_collection(
                db, entity_type, db_config, model_spec, snapshots_config
            )

        self.log = logging.getLogger("EntityDatabase.SnapshotCollections")

    def __getitem__(self, entity_type: str) -> TypedSnapshotCollection:
        if entity_type not in self._snapshot_collections:
            raise KeyError(f"Entity type '{entity_type}' not found in model spec")
        return self._snapshot_collections[entity_type]

    def save_one(self, entity_type: str, snapshot: dict, ctime: datetime) -> None:
        """Save snapshot to specified entity of current master document."""
        return self[entity_type].save_one(snapshot, ctime)

    def save_many(self, entity_type: str, snapshots: list[dict], ctime: datetime) -> None:
        """
        Saves a list of snapshots of current master documents.

        All snapshots must belong to same entity type.
        """
        return self[entity_type].save_many(snapshots, ctime)

    def get_latest_one(self, entity_type: str, eid: AnyEidT) -> dict:
        """Get latest snapshot of given eid.

        If doesn't exist, returns {}.
        """
        return self[entity_type].get_latest_one(eid)

    def get_latest(
        self,
        entity_type: str,
        fulltext_filters: Optional[dict[str, str]] = None,
        generic_filter: Optional[dict[str, Any]] = None,
    ) -> tuple[Cursor, int]:
        """Get latest snapshots of given `etype`.

        This method is useful for displaying data on web.

        Returns only documents matching `generic_filter` and `fulltext_filters`
        (dictionary attribute - fulltext filter).
        Fulltext filters are interpreted as regular expressions.
        Only string values may be filtered this way. There's no validation that queried attribute
        can be fulltext filtered.
        Only plain and observation attributes with string-based data types can be queried.
        Array and set data types are supported as well as long as they are not multi value
        at the same time.
        If you need to filter EIDs, ensure the EID is string, then use attribute `eid`.
        Otherwise, use generic filter.

        Generic filter allows filtering using generic MongoDB query (including `$and`, `$or`,
        `$lt`, etc.).
        For querying non-JSON-native types, you can use magic strings, such as
        `"$$IPv4{<ip address>}"` for IPv4 addresses. The full spec with examples is in the
        [magic strings module][dp3.database.magic].

        Generic and fulltext filters are merged - fulltext overrides conflicting keys.

        Also returns total document count (after filtering).

        May raise `SnapshotCollectionError` if query is invalid.
        """
        return self[entity_type].get_latest(fulltext_filters, generic_filter)

    def find_latest(
        self,
        entity_type: str,
        fulltext_filters: Optional[dict[str, str]] = None,
        generic_filter: Optional[dict[str, Any]] = None,
    ) -> Cursor:
        """Find latest snapshots of given `etype`.

        see [`get_latest`][dp3.database.snapshots.SnapshotCollectionContainer.get_latest]
        for more information.

        Returns only documents matching `generic_filter` and `fulltext_filters`,
        does not count them.
        """
        return self[entity_type].find_latest(fulltext_filters, generic_filter)

    def count_latest(
        self,
        entity_type: str,
        fulltext_filters: Optional[dict[str, str]] = None,
        generic_filter: Optional[dict[str, Any]] = None,
    ) -> int:
        """Count latest snapshots of given `etype`.

        see [`get_latest`][dp3.database.snapshots.SnapshotCollectionContainer.get_latest]
        for more information.

        Returns only count of documents matching `generic_filter` and `fulltext_filters`.

        Note that this method may take much longer than `get_latest` on larger databases,
        as it does count all documents, not just return the first few.
        """
        return self[entity_type].count_latest(fulltext_filters, generic_filter)

    def get_by_eid(
        self,
        entity_type: str,
        eid: AnyEidT,
        t1: Optional[datetime] = None,
        t2: Optional[datetime] = None,
    ) -> Union[Cursor, CommandCursor]:
        """Get all (or filtered) snapshots of given `eid`.

        This method is useful for displaying `eid`'s history on web.

        Args:
            entity_type: name of entity type
            eid: id of entity, to which data-points correspond
            t1: left value of time interval (inclusive)
            t2: right value of time interval (inclusive)
        """
        return self[entity_type].get_by_eid(eid, t1, t2)

    def get_distinct_val_count(self, entity_type: str, attr: str) -> dict[Any, int]:
        """Counts occurrences of distinct values of given attribute in snapshots.

        Returns dictionary mapping value -> count.

        Works for all plain and observation data types except `dict` and `json`.
        """
        return self[entity_type].get_distinct_val_count(attr)

    def delete_old(self, t_old: datetime) -> int:
        """Delete old snapshots, may raise `SnapshotCollectionError`.

        Periodically called from HistoryManager.
        Returns:
             number of deleted snapshots.
        """
        deleted_total = 0
        for collection in self._snapshot_collections.values():
            try:
                deleted_total += collection.delete_old(t_old)
            except Exception as e:
                raise SnapshotCollectionError(f"Delete of old snapshots failed: {e}") from e
        return deleted_total

    def delete_eid(self, entity_type: str, eid: AnyEidT) -> int:
        """Delete snapshots of given `eids`."""
        return self[entity_type].delete_eid(eid)

    def delete_eids(self, entity_type: str, eids: list[Any]) -> int:
        """Delete snapshots of given `eids`."""
        return self[entity_type].delete_eids(eids)
