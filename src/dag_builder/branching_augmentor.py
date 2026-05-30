import random
from typing import List, Optional

import networkx as nx
import numpy as np

from ..branching_validator import BranchingConstraintError, BranchingValidator
from ..common import Util
from ..config import Config
from ..exceptions import BuildFailedError


class BranchingAugmentor:
    """Insert cDAG / pDAG branching constructs into an existing DAG."""

    def __init__(self, config: Config, max_try: int = 100) -> None:
        self._config = config
        self._max_try = max_try
        self._next_node_id: int = 0
        self._next_unit_id: int = 0

    def augment(self, dag: nx.DiGraph, layout_hint: str) -> nx.DiGraph:
        for try_i in range(1, self._max_try + 1):
            self._next_unit_id = 0
            work = dag.copy()
            self._next_node_id = max(work.nodes(), default=-1) + 1
            for n in list(work.nodes()):
                work.nodes[n].setdefault("node_type", "regular")
            if layout_hint == "chain":
                result = self._augment_chain(work, current_depth=0)
            elif layout_hint == "gnp":
                result = self._augment_gnp(work, current_depth=0,
                                           initial_size=work.number_of_nodes())
            else:
                raise ValueError(f"unknown layout_hint: {layout_hint}")
            try:
                BranchingValidator.assert_valid(result, self._config.firing)
                return result
            except BranchingConstraintError:
                if try_i == self._max_try:
                    raise BuildFailedError(
                        f"Branching augmentation failed after {self._max_try} tries"
                    )
                continue

    # ---------- chain mode ----------

    def _augment_chain(self, dag: nx.DiGraph, current_depth: int) -> nx.DiGraph:
        max_depth = Util.random_choice(self._config.maximum_nesting_depth)
        if current_depth >= max_depth:
            return dag
        p_b = Util.random_choice(self._config.probability_of_branching)
        max_branches = Util.random_choice(self._config.maximum_branches)

        candidates = [n for n in list(nx.topological_sort(dag))
                      if dag.nodes[n].get("node_type") == "regular"]
        for v in candidates:
            if v not in dag.nodes():
                continue
            if random.random() >= p_b:
                continue
            k = random.randint(2, max(2, max_branches))
            remaining = self._chain_position_remaining(dag, v)
            L_sub = max(1, remaining)
            self._replace_node_with_branches(dag, v, k, L_sub, current_depth)
        return dag

    def _chain_position_remaining(self, dag: nx.DiGraph, v: int) -> int:
        """Approximate remaining chain length at v, used as subsequence length."""
        try:
            descendants = nx.descendants(dag, v)
            return max(1, len(descendants))
        except nx.NetworkXError:
            return 1

    def _replace_node_with_branches(self, dag: nx.DiGraph, v: int, k: int,
                                    L_sub: int, current_depth: int) -> None:
        unit_id = self._take_unit_id()
        csrc = self._take_node_id()
        csnk = self._take_node_id()
        dag.add_node(csrc, node_type="C_src", branch_unit_id=unit_id, execution_time=0)
        dag.add_node(csnk, node_type="C_snk", branch_unit_id=unit_id, execution_time=0)
        preds = list(dag.predecessors(v))
        succs = list(dag.successors(v))
        dag.remove_node(v)
        for p in preds:
            dag.add_edge(p, csrc)
        for s in succs:
            dag.add_edge(csnk, s)
        probs = self._sample_categorical(k)
        for j in range(k):
            sub_nodes = [self._take_node_id() for _ in range(L_sub)]
            for n in sub_nodes:
                dag.add_node(n, node_type="regular", execution_time=0)
            for a, b in zip(sub_nodes[:-1], sub_nodes[1:]):
                dag.add_edge(a, b)
            attrs = {"branch_id": j}
            if self._config.firing == "probabilistic":
                attrs["firing_prob"] = float(probs[j])
            dag.add_edge(csrc, sub_nodes[0], **attrs)
            dag.add_edge(sub_nodes[-1], csnk)
            sub_view = dag.subgraph(sub_nodes).copy()
            self._augment_chain(sub_view, current_depth + 1)
            # Merge augmented sub_view back into dag (its node ids are unique)
            for n, attr in sub_view.nodes(data=True):
                if n not in dag.nodes():
                    dag.add_node(n, **attr)
                else:
                    dag.nodes[n].update(attr)
            for u, w, attr in sub_view.edges(data=True):
                if not dag.has_edge(u, w):
                    dag.add_edge(u, w, **attr)

    # ---------- gnp mode (Task 4) ----------

    def _augment_gnp(self, dag: nx.DiGraph, current_depth: int,
                     initial_size: int) -> nx.DiGraph:
        raise NotImplementedError("gnp mode implemented in Task 4")

    # ---------- common ----------

    def _take_node_id(self) -> int:
        n = self._next_node_id
        self._next_node_id += 1
        return n

    def _take_unit_id(self) -> int:
        u = self._next_unit_id
        self._next_unit_id += 1
        return u

    def _sample_categorical(self, k: int) -> List[float]:
        dist = self._config.probability_distribution
        if dist == "dirichlet":
            alpha = float(self._config.dirichlet_alpha)
            return list(np.random.dirichlet([alpha] * k))
        elif dist == "uniform-normalize":
            r = [random.uniform(0.0, 1.0) for _ in range(k)]
            s = sum(r)
            return [x / s for x in r]
        else:
            raise ValueError(f"unknown distribution: {dist}")
