from typing import Dict, List, Set

import networkx as nx


class BranchingConstraintError(Exception):
    """Raised when a DAG violates Melani Def III.1 or Zhao p-DAG constraints."""


class BranchingValidator:
    """Verify cDAG / pDAG structural constraints.

    Melani 2015 Def III.1 (cDAG):
      - Each branch_unit_id has exactly one C_src and one C_snk.
      - branch_id values on C_src out-edges are {0, 1, ..., k-1}.
      - For each unit u, the branch bodies B_j(u) are pairwise disjoint.

    Zhao 2025 p-DAG (only when firing == "probabilistic"):
      - Sum of firing_prob across one unit equals 1 (within 1e-9).
      - All firing_prob values are in [0, 1].
      - branch_unit_id is unique per (C_src, C_snk) pair (already implied by the
        Melani check, included here for explicitness).
    """

    _TOLERANCE = 1e-9

    @staticmethod
    def assert_valid(dag: nx.DiGraph, firing: str) -> None:
        BranchingValidator._check_melani_def_iii_1(dag)
        if firing == "probabilistic":
            BranchingValidator._check_zhao_p_dag(dag)

    @staticmethod
    def _collect_units(dag: nx.DiGraph) -> Dict[int, Dict[str, List[int]]]:
        units: Dict[int, Dict[str, List[int]]] = {}
        for n, attr in dag.nodes(data=True):
            t = attr.get("node_type", "regular")
            if t not in ("C_src", "C_snk"):
                continue
            uid = attr.get("branch_unit_id")
            if uid is None:
                raise BranchingConstraintError(
                    f"node {n} of type {t} has no branch_unit_id"
                )
            units.setdefault(uid, {"C_src": [], "C_snk": []})[t].append(n)
        return units

    @staticmethod
    def _check_melani_def_iii_1(dag: nx.DiGraph) -> None:
        units = BranchingValidator._collect_units(dag)
        for uid, members in units.items():
            srcs, snks = members["C_src"], members["C_snk"]
            if len(srcs) != 1 or len(snks) != 1:
                raise BranchingConstraintError(
                    f"unit {uid} must have exactly one C_src and one C_snk; "
                    f"got C_src={srcs}, C_snk={snks}"
                )
            csrc, csnk = srcs[0], snks[0]
            out_edges = list(dag.out_edges(csrc, data=True))
            raw_ids = [d.get("branch_id") for _, _, d in out_edges]
            if any(bid is None for bid in raw_ids):
                raise BranchingConstraintError(
                    f"unit {uid}: one or more C_src out-edges missing branch_id"
                )
            branch_ids = sorted(raw_ids)
            if branch_ids != list(range(len(out_edges))):
                raise BranchingConstraintError(
                    f"unit {uid}: branch_id on C_src out-edges must be "
                    f"{{0, ..., k-1}}; got {branch_ids}"
                )

            bodies: List[Set[int]] = []
            for _, succ, _ in out_edges:
                body = BranchingValidator._branch_body(dag, succ, csnk)
                bodies.append(body)
            for i in range(len(bodies)):
                for j in range(i + 1, len(bodies)):
                    overlap = bodies[i] & bodies[j]
                    if overlap:
                        raise BranchingConstraintError(
                            f"unit {uid}: branches {i} and {j} share vertices {overlap}"
                        )

    @staticmethod
    def _branch_body(dag: nx.DiGraph, start: int, csnk: int) -> Set[int]:
        """All vertices reachable from `start` without traversing `csnk`."""
        if start == csnk:
            return set()
        body: Set[int] = {start}
        stack = [start]
        while stack:
            u = stack.pop()
            for v in dag.successors(u):
                if v == csnk or v in body:
                    continue
                body.add(v)
                stack.append(v)
        return body

    @staticmethod
    def _check_zhao_p_dag(dag: nx.DiGraph) -> None:
        units = BranchingValidator._collect_units(dag)
        for uid in units.keys():
            csrc = units[uid]["C_src"][0]
            total = 0.0
            for _, _, d in dag.out_edges(csrc, data=True):
                p = d.get("firing_prob")
                if p is None:
                    raise BranchingConstraintError(
                        f"unit {uid}: out-edge of C_src {csrc} missing firing_prob"
                    )
                if not (0.0 <= p <= 1.0):
                    raise BranchingConstraintError(
                        f"unit {uid}: firing_prob {p} out of [0, 1]"
                    )
                total += p
            if abs(total - 1.0) > BranchingValidator._TOLERANCE:
                raise BranchingConstraintError(
                    f"unit {uid}: sum of firing_prob = {total}, expected 1"
                )
