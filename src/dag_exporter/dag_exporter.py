import json
import subprocess

import networkx as nx
import yaml
from networkx.readwrite import json_graph

from ..config import Config


class DAGExporter:
    """DAG exporter class."""

    def __init__(self, config: Config) -> None:
        self._config = config

    def export(self, dag: nx.DiGraph, dest_dir: str, file_name: str) -> None:
        """Export DAG.

        Parameters
        ----------
        dag : nx.DiGraph
            DAG.
        dest_dir : str
            Destination directory.
        file_name : str
            File name.

        """
        self._export_dag(dag, dest_dir, file_name)
        if self._config.figure:
            self._export_fig(dag, dest_dir, file_name)
        if self._config.export_constraints:
            self._export_constraints(dag, dest_dir, file_name)

    def _export_dag(self, dag: nx.DiGraph, dest_dir: str, file_name: str) -> None:
        """Export DAG description file.

        Supported extension: [YAML/JSON/DOT/XML].

        Parameters
        ----------
        dag : nx.DiGraph
            DAG.
        dest_dir : str
            Destination directory.
        file_name : str
            File name.

        """
        if self._config.yaml:
            data = json_graph.node_link_data(dag)
            s = json.dumps(data)
            dic = json.loads(s)
            with open(f"{dest_dir}/{file_name}.yaml", "w") as f:
                yaml.dump(dic, f)

        if self._config.json:
            data = json_graph.node_link_data(dag)
            s = json.dumps(data)
            with open(f"{dest_dir}/{file_name}.json", "w") as f:
                json.dump(s, f)

        if self._config.dot:
            nx.drawing.nx_pydot.write_dot(dag, f"{dest_dir}/{file_name}.dot")

        if self._config.xml:
            nx.write_graphml_xml(dag, f"{dest_dir}/{file_name}.xml")

    def _export_fig(self, dag: nx.DiGraph, dest_dir: str, file_name: str) -> None:
        """Export DAG figure.

        Supported extension: [PNG/PDF/EPS/SVG].

        Parameters
        ----------
        dag : nx.DiGraph
            DAG.
        dest_dir : str
            Destination directory.
        file_name : str
            File name.

        """
        # Preprocessing
        for node_i in dag.nodes():
            dag.nodes[node_i]["label"] = (
                f"[{node_i}]\n" f'C: {dag.nodes[node_i]["execution_time"]}'
            )
            if period := dag.nodes[node_i].get("period"):
                dag.nodes[node_i]["shape"] = "box"
                dag.nodes[node_i]["label"] += f"\nT: {period}"
            if deadline := dag.nodes[node_i].get("end_to_end_deadline"):
                dag.nodes[node_i]["style"] = "bold"
                dag.nodes[node_i]["label"] += f"\nD: {deadline}"

        for src_i, tgt_i in dag.edges():
            if comm := dag.edges[src_i, tgt_i].get("communication_time"):
                dag.edges[src_i, tgt_i]["label"] = f" {comm}"
                dag.edges[src_i, tgt_i]["fontsize"] = 10

        # Add legend
        if self._config.draw_legend:
            legend_str = [
                "----- Legend ----\n\n",
                "Circle node:  Event-driven node\l",
                "[i]:  Task index\l",
                "C:  Worst-case execution time (WCET)\l",
            ]
            if self._config.multi_rate:
                legend_str.insert(1, "Square node:  Timer-driven node\l")
                legend_str.append("T:  Period\l")
            if self._config.end_to_end_deadline:
                legend_str.append("D:  End-to-end deadline\l")
            if self._config.communication_time:
                legend_str.append("Number attached to arrow:  Communication time\l")
            dag.add_node(-1, label="".join(legend_str), fontsize=15, shape="box3d")

        # Export
        pdot = nx.drawing.nx_pydot.to_pydot(dag)
        if self._config.png:
            pdot.write_png(f"{dest_dir}/{file_name}.png")
        if self._config.svg:
            pdot.write_svg(f"{dest_dir}/{file_name}.svg")
        if self._config.pdf:
            pdot.write_pdf(f"{dest_dir}/{file_name}.pdf")
        if self._config.eps:
            pdot.write_ps(f"{dest_dir}/{file_name}.ps")
            subprocess.run(
                f"eps2eps {dest_dir}/{file_name}.ps {dest_dir}/{file_name}.eps \
                && rm {dest_dir}/{file_name}.ps",
                shell=True,
            )

    def _export_constraints(self, dag: nx.DiGraph, dest_dir: str, file_name: str) -> None:
        """Export constraints.

        Parameters
        ----------
        dag : nx.DiGraph
            DAG.
        dest_dir : str
            Destination directory.
        file_name : str
            File name.

        """
        conf = self._config.export_constraints

        N = len(dag.nodes)
        periods = [dag.nodes[i]["period"] for i in range(N)]
        execution_times = [dag.nodes[i]["execution_time"] for i in range(N)]
        tails = [node for node, out_degree in dag.out_degree() if out_degree == 0]

        child = {}

        for src, tgt in dag.edges:
            if src not in child:
                child[src] = []

            child[src].append(tgt)

        topo_sort = list(nx.topological_sort(dag))
        # topo_all = list(nx.all_topological_sorts(dag))

        V_multiplier = conf.get("Freshness Multiplier", 2)
        V = [V_multiplier * periods[i] for i in range(N-1)]


        buf = []
        if conf.get("Number of Nodes", False):
            buf.append(f"{N}")
        buf.append("")
        
        if conf.get("Periods", False):
            buf.append(" ".join(map(str, periods)))
        if conf.get("Execution Times", False):
            buf.append(" ".join(map(str, execution_times)))
        if conf.get("Freshness", False):
            buf.append(" ".join(map(str, V)))
        # if conf.get("Children", False):
        
        if conf.get("Children", False):
            buf.append("")
            buf.append(f"{len(child)}")
            for i in child.keys():
                buf.append(f"{i} {len(child[i])} {' '.join(map(str, child[i]))}")

        if conf.get("Pathways", False):
            buf.append("")
            all_pathway = []
            for head in dag.nodes:
                for tail in tails:
                    all_simple_paths = list(nx.all_simple_paths(dag, head, tail))
                    # last element should be the head
                    for i in range(len(all_simple_paths)):
                        all_simple_paths[i][-1] = head
                        all_simple_paths[i] = list(map(int, all_simple_paths[i]))

                    all_pathway.extend(all_simple_paths)

            for i in all_pathway:
                if len(i) == 1:
                    all_pathway.remove(i)

            buf.append(f"{len(all_pathway)}")
            for i in all_pathway:
                buf.append(f"{' '.join(map(str, i))}")
            
        if conf.get("Topological Order", False):
            buf.append("")
            buf.append(" ".join(map(str, topo_sort)))

        with open(f"{dest_dir}/{file_name}.txt", "w") as f:
            f.write("\n".join(buf))
            
            


        

