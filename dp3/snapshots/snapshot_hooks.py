import logging
from collections import defaultdict
from collections.abc import Hashable
from dataclasses import dataclass, field
from itertools import combinations
from typing import Callable

from dp3.common.attrspec import (
    AttrSpecObservations,
    AttrSpecPlain,
    AttrSpecTimeseries,
    AttrType,
)
from dp3.common.config import ModelSpec
from dp3.common.task import Task


class SnapshotTimeseriesHookContainer:
    """Container for timeseries analysis hooks"""

    def __init__(self, log: logging.Logger, model_spec: ModelSpec):
        self.log = log.getChild("timeseriesHooks")
        self.model_spec = model_spec

        self._hooks = defaultdict(list)

    def register(
        self, hook: Callable[[str, str, dict], list[Task]], entity_type: str, attr_type: str
    ):
        """
        Registers passed hook to be called during snapshot creation.

        Binds hook to specified entity_type and attr_type (though same hook can be bound
        multiple times).
        `hook` callable should expect entity_type, attr_type and attribute history as arguments
        and return a list of `Task` objects.

        If entity_type and attr_type do not specify a valid timeseries attribute,
        a ValueError is raised.
        """
        if (entity_type, attr_type) not in self.model_spec.attributes:
            raise ValueError(f"Attribute '{attr_type}' of entity '{entity_type}' does not exist.")
        spec = self.model_spec.attributes[entity_type, attr_type]
        if spec.t != AttrType.TIMESERIES:
            raise ValueError(f"'{entity_type}.{attr_type}' is not a timeseries, but '{spec.t}'")
        self._hooks[entity_type, attr_type].append(hook)
        self.log.debug(f"Added hook: '{hook.__qualname__}'")

    def run(self, entity_type: str, attr_type: str, attr_history: dict) -> list[Task]:
        """Runs registered hooks."""
        tasks = []
        for hook in self._hooks[entity_type, attr_type]:
            try:
                new_tasks = hook(entity_type, attr_type, attr_history)
                tasks.extend(new_tasks)
            except Exception as e:
                self.log.error(f"Error during running hook {hook}: {e}")
        return tasks


class SnapshotCorrelationHookContainer:
    """Container for data fusion and correlation hooks."""

    def __init__(self, log: logging.Logger, model_spec: ModelSpec):
        self.log = log.getChild("CorrelationHooks")
        self.model_spec = model_spec

        self._hooks = defaultdict(list)

        self._dependency_graph = DependencyGraph(self.log)

    def register(
        self,
        hook: Callable,
        entity_type: str,
        depends_on: list[list[str]],
        may_change: list[list[str]],
    ):
        """
        Registers passed hook to be called during snapshot creation.

        Binds hook to specified entity_type (though same hook can be bound multiple times).

        If entity_type and attribute specifications are validated
        and ValueError is raised on failure.
        :param hook: `hook` callable should expect ... TODO
        :param entity_type: specifies entity type
        :param depends_on: each item should specify an attribute that is depended on
         in the form of a path from the specified entity_type to individual attributes
         (even on linked entities).
        :param may_change: each item should specify an attribute that `hook` may change.
         specification format is identical to `depends_on`.
        """

        if entity_type not in self.model_spec.entities:
            raise ValueError(f"Entity '{entity_type}' does not exist.")

        self._validate_attr_paths(entity_type, depends_on)
        self._validate_attr_paths(entity_type, may_change)

        depends_on = self._expand_path_backlinks(entity_type, depends_on)
        may_change = self._expand_path_backlinks(entity_type, may_change)

        depends_on = self._embed_base_entity(entity_type, depends_on)
        may_change = self._embed_base_entity(entity_type, may_change)

        hook_id = (
            f"{hook.__qualname__}("
            f"{entity_type}, [{','.join(depends_on)}], [{','.join(may_change)}]"
            f")"
        )
        self._dependency_graph.add_hook_dependency(hook_id, depends_on, may_change)

        self._hooks[entity_type].append(hook)
        self.log.debug(f"Added hook: '{hook_id}'")

    def run(self, entity_type: str, master_record: dict):
        """Runs registered hooks."""

    def _validate_attr_paths(self, base_entity: str, paths: list[list[str]]):
        entity_attributes = self.model_spec.entity_attributes
        for path in paths:
            position = entity_attributes[base_entity]
            for step in path:
                if step not in position:
                    raise ValueError(
                        f"Invalid path '{'->'.join([base_entity] + path)}', failed on '{step}'"
                    )
                position = position[step]
                if position.is_relation:
                    position = entity_attributes[position.relation_to]
            assert isinstance(position, (AttrSpecPlain, AttrSpecObservations, AttrSpecTimeseries))

    def _expand_path_backlinks(self, base_entity: str, paths: list[list[str]]):
        """
        Returns a list of all possible subpaths considering the path backlinks.

        With user defined entities, attributes and dependency paths, presence of backlinks (cycles)
        in specified dependency paths must be expected. To fully track dependencies,
        we assume any entity repeated in the path can be referenced multiple times,
        effectively making a cycle in the path, which can be ignored.
        """
        expanded_paths = []
        for path in paths:
            resolved_path = self._resolve_entities_in_path(base_entity, path)
            expanded = [resolved_path] + self._catch_them_all(resolved_path)
            expanded_paths.extend(
                self._extract_path_from_resolved(resolved) for resolved in expanded
            )
        unique_paths = {tuple(path) for path in expanded_paths}
        expanded_paths = sorted(list(list(path) for path in unique_paths), key=lambda x: len(x))
        return expanded_paths

    def _resolve_entities_in_path(self, base_entity: str, path: list[str]) -> list[tuple[str, str]]:
        entity_attributes = self.model_spec.entity_attributes
        position = entity_attributes[base_entity]
        resolved_path = []
        for step in path:
            resolved_path.append((base_entity, step))
            position = position[step]
            if position.is_relation:
                base_entity = position.relation_to
                position = entity_attributes[position.relation_to]
        return resolved_path

    @staticmethod
    def _extract_path_from_resolved(path: list[tuple[str, str]]) -> list[str]:
        return [attr for entity, attr in path]

    def _catch_them_all(self, path: list[tuple[str, str]]) -> list[list[tuple[str, str]]]:
        root_cycles = self._get_root_cycles(path)
        out = []
        for beg, end in root_cycles:
            pre = path[:beg]
            post = path[end:]

            inner_cycles = self._catch_them_all(path[beg:end])
            out.extend(pre + inner + post for inner in inner_cycles)
            out.append(pre + post)
        return out

    def _get_root_cycles(self, path: list[tuple[str, str]]) -> list[tuple[int, int]]:
        attributes = self.model_spec.attributes
        entity_indexes: defaultdict[str, list[int]] = defaultdict(list)
        for i, step in enumerate(path):
            entity, attr = step
            if attributes[entity, attr].is_relation:
                entity_indexes[entity].append(i)

        if not any(len(indexes) > 1 for indexes in entity_indexes.values()):
            return []

        possible_backlinks = [
            combination
            for indexes in entity_indexes.values()
            for combination in combinations(indexes, 2)
        ]
        return [
            (curr_beg, curr_end)
            for curr_beg, curr_end in possible_backlinks
            if not any(beg < curr_beg and curr_end < end for beg, end in possible_backlinks)
        ]

    @staticmethod
    def _embed_base_entity(base_entity: str, paths: list[list[str]]):
        return ["->".join([base_entity] + path) for path in paths]


@dataclass
class GraphVertex:
    """Vertex in a graph of dependencies"""

    adj: list = field(default_factory=list)
    in_degree: int = 0
    type: str = "attr"


class DependencyGraph:
    """Class representing a graph of dependencies between correlation hooks."""

    def __init__(self, log):
        self.log = log.getChild("DependencyGraph")

        # dictionary of adjacency lists for each edge
        self._vertices = defaultdict(GraphVertex)

    def add_hook_dependency(self, hook_id: str, depends_on: list[str], may_change: list[str]):
        """Add hook to dependency graph and recalculate if any cycles are created."""
        if hook_id in self._vertices:
            raise ValueError(f"Hook id '{hook_id}' already present in the vertices.")
        for path in depends_on:
            self.add_edge(path, hook_id)
        for path in may_change:
            self.add_edge(hook_id, path)
        self._vertices[hook_id].type = "hook"
        try:
            self.topological_sort()
        except ValueError as err:
            raise ValueError(f"Hook {hook_id} introduces a circular dependency.") from err
        self.check_multiple_writes()

    def add_edge(self, id_from: Hashable, id_to: Hashable):
        """Add oriented edge between specified vertices."""
        self._vertices[id_from].adj.append(id_to)
        # Ensure vertex with 'id_to' exists to avoid iteration errors later.
        _ = self._vertices[id_to]

    def calculate_in_degrees(self):
        """Calculate number of incoming edges for each vertex. Time complexity O(V + E)."""
        for vertex_node in self._vertices.values():
            vertex_node.in_degree = 0

        for vertex_node in self._vertices.values():
            for adjacent_name in vertex_node.adj:
                self._vertices[adjacent_name].in_degree += 1

    def topological_sort(self):
        """
        Implementation of Kahn's algorithm for topological sorting.
        Raises ValueError if there is a cycle in the graph.

        See https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
        """
        self.calculate_in_degrees()
        queue = [(node_id, node) for node_id, node in self._vertices.items() if node.in_degree == 0]
        topological_order = []
        processed_vertices_cnt = 0

        while queue:
            curr_node_id, curr_node = queue.pop(0)
            topological_order.append(curr_node_id)

            # Decrease neighbouring nodes' in-degree by 1
            for neighbor in curr_node.adj:
                neighbor_node = self._vertices[neighbor]
                neighbor_node.in_degree -= 1
                # If in-degree becomes zero, add it to queue
                if neighbor_node.in_degree == 0:
                    queue.append((neighbor, neighbor_node))

            processed_vertices_cnt += 1

        if processed_vertices_cnt != len(self._vertices):
            raise ValueError("Dependency graph contains a cycle.")
        else:
            return topological_order

    def check_multiple_writes(self):
        self.calculate_in_degrees()
        multiple_writes = [
            vertex_id
            for vertex_id, vertex in self._vertices.items()
            if vertex.in_degree > 1 and vertex.type == "attr"
        ]
        if multiple_writes:
            self.log.warn(
                "Detected possible over-write of hook results in attributes: %s", multiple_writes
            )
