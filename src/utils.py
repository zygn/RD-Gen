import networkx as nx
import argparse
import random
from typing import List, Tuple


def option_parser() -> Tuple[argparse.FileType, str]:
    usage = f'[python] {__file__} \
              --config_yaml_path [<path to config file>] \
              --dest_dir [<destination directory>]'

    arg_parser = argparse.ArgumentParser(usage=usage)
    arg_parser.add_argument('--config_yaml_path',
                            type=argparse.FileType(("r")),
                            help='config file name (.yaml)')
    arg_parser.add_argument('--dest_dir',
                            type=str,
                            default='./',
                            help='destination directory')
    args = arg_parser.parse_args()

    return args.config_yaml_path, args.dest_dir


def _get_cp_and_cp_len(dag: nx.DiGraph, source, exit) -> Tuple[List[int], int]:
    cp = []
    cp_len = 0

    paths = nx.all_simple_paths(dag, source=source, target=exit)
    for path in paths:
        path_len = 0
        for i in range(len(path)):
            path_len += dag.nodes[path[i]]['execution_time']
            if(i != len(path)-1 and
                    'communication_time' in list(dag.nodes[path[i]].keys())):
                path_len += dag.edges[path[i], path[i+1]]['communication_time']
        if(path_len > cp_len):
            cp = path
            cp_len = path_len

    return cp, cp_len


def set_end_to_end_deadlines(conf, G: nx.DiGraph) -> None:
    if('Use multi-period' in conf.keys() and
            'Ratio of deadlines to max period' in conf['Use end-to-end deadline'].keys()):
        max_period = max((nx.get_node_attributes(G, 'period')).values())
        for exit_i in [v for v, d in G.out_degree() if d == 0]:
            G.nodes[exit_i]['deadline'] = \
                    int(max_period * conf['Use end-to-end deadline']['Ratio of deadlines to max period'])

    elif('Ratio of deadlines to critical path length' in conf['Use end-to-end deadline'].keys()):
        for exit_i in [v for v, d in G.out_degree() if d == 0]:
            max_cp_len = 0
            for entry_i in [v for v, d in G.in_degree() if d == 0]:
                _, cp_len = _get_cp_and_cp_len(G, entry_i, exit_i)
                if(cp_len > max_cp_len):
                    max_cp_len = cp_len
            G.nodes[exit_i]['deadline'] = int(
                    max_cp_len
                    * conf['Use end-to-end deadline']['Ratio of deadlines to critical path length'])


def random_get_comm_time(conf) -> int:
    if('Use list' in conf['Use communication time'].keys()):
        return random.choice(conf['Use communication time']['Use list'])
    else:
        return random.randint(conf['Use communication time']['Min'],
                              conf['Use communication time']['Max'])


def random_get_exec_time(conf) -> int:
    if('Use list' in conf['Execution time'].keys()):
        return random.choice(conf['Execution time']['Use list'])
    else:
        return random.randint(conf['Execution time']['Min'],
                              conf['Execution time']['Max'])


def _get_settable_min_exec(conf) -> int:
    if('Use list' in conf['Execution time'].keys()):
        return min(conf['Execution time']['Use list'])
    else:
        return conf['Execution time']['Min']


def _get_settable_min_comm(conf) -> int:
    if('Use list' in conf['Use communication time'].keys()):
        return min(conf['Use communication time']['Use list'])
    else:
        return conf['Use communication time']['Min']


def _get_settable_max_period(conf) -> int:
    if('Use list' in conf['Use multi-period'].keys()):
        return max(conf['Use multi-period']['Use list'])
    else:
        return conf['Use multi-period']['Max']