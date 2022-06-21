import os
import pandas as pd
import rdflib
from rdflib import Graph
import networkx as nx

# These lines allows to make sure that we are placed at the repo directory level 
from pathlib import Path

p = Path(__file__).parents[2]
os.chdir(p)

data_in_path = './data/in/vgf_unaligned_data_test/'
g = Graph()
nm = g.namespace_manager

# Create jlw namespace
jlw_uri = "https://www.sinergiawolfender.org/jlw/"
ns_jlw = rdflib.Namespace(jlw_uri)
prefix = "mkg"
nm.bind(prefix, ns_jlw)

path = os.path.normpath(data_in_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in samples_dir:
    graph_path = os.path.join(path, directory, directory + '_mn_pos_mn.graphml')
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
        s_feature_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_feature_" + str(s))
        t_feature_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_feature_" + str(t))
        g.add((s_feature_id, ns_jlw.is_cosine_similar_to, t_feature_id))

pathout = os.path.normpath("./data/out/")
os.makedirs(pathout, exist_ok=True)
g.serialize(destination="./data/out/individual_mn.ttl", format="ttl", encoding="utf-8")      