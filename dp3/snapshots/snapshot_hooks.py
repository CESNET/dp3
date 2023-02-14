import logging
from collections import defaultdict
from collections.abc import Hashable
from dataclasses import dataclass, field
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
        self.log = log.getChild("correlationHooks")
        self.model_spec = model_spec

        self._hooks = defaultdict(list)

        self._dependency_graph = DependencyGraph()

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

        hook_id = (
            f"{hook.__qualname__}("
            f"{entity_type}, "
            f"[{','.join('->'.join(path) for path in depends_on)}], "
            f"[{','.join('->'.join(path) for path in may_change)}])"
        )
        self._dependency_graph.add_hook_dependency(hook_id, depends_on, may_change)

        self._hooks[entity_type].append(hook)
        self.log.debug(f"Added hook: '{hook_id}'")

    def run(self, entity_type: str, master_record: dict):
        """Runs registered hooks."""

    def _validate_attr_paths(self, base_enity: str, paths: list[list[str]]):
        entity_attributes = self.model_spec.entity_attributes
        for path in paths:
            position = entity_attributes[base_enity]
            for step in path:
                if step not in position:
                    raise ValueError(
                        f"Invalid path '{' -> '.join([base_enity] + path)}', failed on '{step}'"
                    )
                position = position[step]
                if position.is_relation:
                    position = entity_attributes[position.relation_to]
            assert isinstance(position, (AttrSpecPlain, AttrSpecObservations, AttrSpecTimeseries))


@dataclass
class GraphVertex:
    """Vertex in a graph of dependencies"""

    adj: list = field(default_factory=list)
    in_degree: int = 0


class DependencyGraph:
    """Class representing a graph of dependencies between correlation hooks."""

    def __init__(self):
        # dictionary of adjacency lists for each edge
        self._vertices = defaultdict(GraphVertex)

    def add_hook_dependency(
        self, hook_id: str, depends_on: list[list[str]], may_change: list[list[str]]
    ):
        """Add hook to dependency graph and recalculate if any cycles are created."""
        if hook_id in self._vertices:
            raise ValueError(f"Hook id '{hook_id}' already present in the vertices.")
        for path in depends_on:
            self.add_edge(tuple(path), hook_id)
        for path in may_change:
            self.add_edge(hook_id, tuple(path))
        try:
            self.topological_sort()
        except ValueError as err:
            raise ValueError(f"Hook {hook_id} introduces a circular dependency.") from err

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
