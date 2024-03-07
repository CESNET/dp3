"""
Module managing registered hooks and their dependencies on one another.
"""

import logging
from collections import defaultdict
from collections.abc import Hashable
from dataclasses import dataclass, field
from typing import Callable, Union

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec
from dp3.common.task import DataPointTask, task_context
from dp3.common.utils import get_func_name
from dp3.task_processing.task_hooks import EventGroupType


class SnapshotTimeseriesHookContainer:
    """Container for timeseries analysis hooks"""

    def __init__(self, log: logging.Logger, model_spec: ModelSpec, elog: EventGroupType):
        self.log = log.getChild("TimeseriesHooks")
        self.elog = elog
        self.model_spec = model_spec

        self._hooks = defaultdict(list)

    def register(
        self,
        hook: Callable[[str, str, list[dict]], list[DataPointTask]],
        entity_type: str,
        attr_type: str,
    ):
        """
        Registers passed timeseries hook to be called during snapshot creation.

        Binds hook to specified entity_type and attr_type (though same hook can be bound
        multiple times).
        If entity_type and attr_type do not specify a valid timeseries attribute,
        a ValueError is raised.
        Args:
            hook: `hook` callable should expect entity_type, attr_type and attribute
                history as arguments and return a list of `Task` objects.
            entity_type: specifies entity type
            attr_type: specifies attribute type
        """
        if (entity_type, attr_type) not in self.model_spec.attributes:
            raise ValueError(f"Attribute '{attr_type}' of entity '{entity_type}' does not exist.")
        spec = self.model_spec.attributes[entity_type, attr_type]
        if spec.t != AttrType.TIMESERIES:
            raise ValueError(f"'{entity_type}.{attr_type}' is not a timeseries, but '{spec.t}'")
        self._hooks[entity_type, attr_type].append(hook)
        self.log.debug(f"Added hook: '{get_func_name(hook)}'")

    def run(
        self, entity_type: str, attr_type: str, attr_history: list[dict]
    ) -> list[DataPointTask]:
        """Runs registered hooks."""
        tasks = []
        with task_context(self.model_spec):
            for hook in self._hooks[entity_type, attr_type]:
                try:
                    new_tasks = hook(entity_type, attr_type, attr_history)
                    tasks.extend(new_tasks)
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook}: {e}")
        return tasks


class SnapshotCorrelationHookContainer:
    """Container for data fusion and correlation hooks."""

    def __init__(self, log: logging.Logger, model_spec: ModelSpec, elog: EventGroupType):
        self.log = log.getChild("CorrelationHooks")
        self.elog = elog
        self.model_spec = model_spec

        self._hooks: defaultdict[str, list[tuple[str, Callable]]] = defaultdict(list)
        self._short_hook_ids: dict = {}

        self._dependency_graph = DependencyGraph(self.log)
        self.used_links = set()

    def register(
        self,
        hook: Callable[[str, dict], Union[None, list[DataPointTask]]],
        entity_type: str,
        depends_on: list[list[str]],
        may_change: list[list[str]],
    ) -> str:
        """
        Registers passed hook to be called during snapshot creation.

        Binds hook to specified entity_type (though same hook can be bound multiple times).

        If entity_type and attribute specifications are validated
        and ValueError is raised on failure.
        Args:
            hook: `hook` callable should expect entity type as str
                and its current values, including linked entities, as dict.
                Can optionally return a list of DataPointTask objects to perform.
            entity_type: specifies entity type
            depends_on: each item should specify an attribute that is depended on
                in the form of a path from the specified entity_type to individual attributes
                (even on linked entities).
            may_change: each item should specify an attribute that `hook` may change.
                specification format is identical to `depends_on`.
        Returns:
            Generated hook id.
        """

        if entity_type not in self.model_spec.entities:
            raise ValueError(f"Entity '{entity_type}' does not exist.")

        self.used_links |= self._validate_attr_paths(entity_type, depends_on)
        self.used_links |= self._validate_attr_paths(entity_type, may_change)

        depends_on = self._get_attr_path_destinations(entity_type, depends_on)
        may_change = self._get_attr_path_destinations(entity_type, may_change)

        hook_args = f"({entity_type}, [{','.join(depends_on)}], [{','.join(may_change)}])"
        hook_id = f"{get_func_name(hook)}{hook_args}"
        self._short_hook_ids[hook_id] = hook_args
        self._dependency_graph.add_hook_dependency(hook_id, depends_on, may_change)

        self._hooks[entity_type].append((hook_id, hook))
        self._restore_hook_order(self._hooks[entity_type])

        self.log.info(f"Added hook: '{hook_id}'")
        return hook_id

    def _validate_attr_paths(
        self, base_entity: str, paths: list[list[str]]
    ) -> set[tuple[str, str]]:
        """
        Validates paths of links between entity attributes

        Returns: The set of used links.
        """
        entity_attributes = self.model_spec.entity_attributes
        used_links = set()
        for path in paths:
            curr_entity = base_entity
            position = entity_attributes[base_entity]
            for i, step in enumerate(path):
                if step not in position:
                    raise ValueError(
                        f"Invalid path '{'->'.join([base_entity] + path)}', failed on '{step}'"
                    )
                position = position[step]
                if position.is_relation:
                    if i != len(path) - 1:
                        used_links.add((curr_entity, step))
                    curr_entity = position.relation_to
                    position = entity_attributes[position.relation_to]
        return used_links

    def _get_attr_path_destinations(self, base_entity: str, paths: list[list[str]]) -> list[str]:
        """
        Normalize paths to contain only destination attributes.
        Returns:
            List of destination attributes as tuples (entity, attr).
        """
        destinations = []
        for path in paths:
            resolved_path = self._resolve_entities_in_path(base_entity, path)
            dest_entity, dest_attr = resolved_path[-1]
            destinations.append(f"{dest_entity}.{dest_attr}")
        return destinations

    def _resolve_entities_in_path(self, base_entity: str, path: list[str]) -> list[tuple[str, str]]:
        """
        Resolves attributes in the path to tuples of  (entity_name, attr_name).

        Args:
            base_entity: Entity to which the first attribute belongs.
            path: List of attribute names, all must be link type except for the last one.

        Returns:
            A resolved link path of tuples (entity_name, attr_name).
        """
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

    def run(self, entities: dict) -> list[DataPointTask]:
        """Runs registered hooks."""
        entity_types = {etype for etype, _ in entities}
        hook_subset = [
            (hook_id, hook, etype) for etype in entity_types for hook_id, hook in self._hooks[etype]
        ]
        topological_order = self._dependency_graph.topological_order
        hook_subset.sort(key=lambda x: topological_order.index(x[0]))
        entities_by_etype = defaultdict(dict)
        for (etype, eid), values in entities.items():
            entities_by_etype[etype][eid] = values

        created_tasks = []

        with task_context(self.model_spec):
            for hook_id, hook, etype in hook_subset:
                short_id = hook_id if len(hook_id) < 160 else self._short_hook_ids[hook_id]
                for eid, entity_values in entities_by_etype[etype].items():
                    self.log.debug("Running hook %s on entity %s", short_id, eid)
                    try:
                        tasks = hook(etype, entity_values)
                        if tasks is not None and tasks:
                            created_tasks.extend(tasks)
                    except Exception as e:
                        self.elog.log("module_error")
                        self.log.error(f"Error during running hook {hook_id}: {e}")
                        self.log.exception(e)

        return created_tasks

    def _restore_hook_order(self, hooks: list[tuple[str, Callable]]):
        topological_order = self._dependency_graph.topological_sort()
        hooks.sort(key=lambda x: topological_order.index(x[0]))


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
        self.topological_order = []

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
            self.topological_order = topological_order
            return topological_order

    def check_multiple_writes(self):
        self.calculate_in_degrees()
        multiple_writes = [
            vertex_id
            for vertex_id, vertex in self._vertices.items()
            if vertex.in_degree > 1 and vertex.type == "attr"
        ]
        if multiple_writes:
            self.log.warning(
                "Detected possible over-write of hook results in attributes: %s", multiple_writes
            )
