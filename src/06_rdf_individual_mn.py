import os
import pandas as pd
import rdflib
from rdflib import Graph
import networkx as nx
import argparse
import textwrap
from pathlib import Path
from tqdm import tqdm

p = Path(__file__).parents[1]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a RDF graph (.ttl format) from samples' metadata 
         --------------------------------
            Arguments:
            - Path to the directory where samples folders are located
            - Ionization mode to process
        '''))

parser.add_argument('-p', '--sample_dir_path', required=True,
                    help='The path to the directory where samples folders to process are located')
parser.add_argument('-ion', '--ionization_mode', required=True,
                    help='The ionization mode to process')

args = parser.parse_args()
sample_dir_path = os.path.normpath(args.sample_dir_path)
ionization_mode = args.ionization_mode

g = Graph()
nm = g.namespace_manager

# Create jlw namespace
jlw_uri = "https://www.sinergiawolfender.org/jlw/"
ns_jlw = rdflib.Namespace(jlw_uri)
prefix = "enpkg"
nm.bind(prefix, ns_jlw)

path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in tqdm(samples_dir):
    graph_path = os.path.join(path, directory, ionization_mode, 'molecular_network', directory + '_mn_' + ionization_mode + '.graphml')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        graph = nx.read_graphml(graph_path)
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    for node in graph.edges(data=True):
        s = node[0]
        t = node[1]
        # s_feature_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_feature_" + str(s) + '_' + ionization_mode)
        # t_feature_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_feature_" + str(t) + '_' + ionization_mode)
        # g.add((s_feature_id, ns_jlw.is_cosine_similar_to, t_feature_id))        
        usi_s = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode + '.mgf:scan:' + str(s) 
        s_feature_id = rdflib.term.URIRef(jlw_uri + 'feature_' + usi_s)
        usi_t = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode + '.mgf:scan:' + str(t) 
        t_feature_id = rdflib.term.URIRef(jlw_uri + 'feature_' + usi_t)
        g.add((s_feature_id, ns_jlw.is_cosine_similar_to, t_feature_id))

pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, f'indivual_mn_{ionization_mode}.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Results are in : {pathout}')   