import os
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF, XSD, RDFS
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
        This script generate a RDF graph (.ttl format) from samples' individual MNs 
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
kg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(kg_uri)
prefix = "enpkg"
nm.bind(prefix, ns_kg)

g.add((ns_kg.LFpair, RDFS.subClassOf, ns_kg.SpectralPair))
g.add((ns_kg.has_member_1, RDFS.subPropertyOf, ns_kg.has_member))
g.add((ns_kg.has_member_2, RDFS.subPropertyOf, ns_kg.has_member))

path = os.path.normpath(sample_dir_path)
pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)

samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in tqdm(samples_dir):
    graph_path = os.path.join(path, directory, ionization_mode, 'molecular_network', directory + '_mn_' + ionization_mode + '.graphml')
    graph_metadata_path = os.path.join(path, directory, ionization_mode, 'molecular_network', directory + '_mn_metadata_' + ionization_mode + '.tsv')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        graph = nx.read_graphml(graph_path)
        metadata = pd.read_csv(metadata_path, sep='\t')
        graph_metadata = pd.read_csv(graph_metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    for node in graph.edges(data=True):
        s = node[0]
        t = node[1]
        cosine = node[2]['weight']
        
        mass_diff = abs(float(graph_metadata[graph_metadata.feature_id == int(s)]['precursor_mz'].values[0] - graph_metadata[graph_metadata.feature_id == int(t)]['precursor_mz'].values[0]))
        component_index = graph_metadata[graph_metadata.feature_id == int(s)]['component_id'].values[0]

        usi_s = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode + '.mgf:scan:' + str(s) 
        s_feature_id = rdflib.term.URIRef(kg_uri + 'lcms_feature_' + usi_s)
        usi_t = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode + '.mgf:scan:' + str(t) 
        t_feature_id = rdflib.term.URIRef(kg_uri + 'lcms_feature_' + usi_t)
        
        ci_node = rdflib.term.URIRef(kg_uri + metadata.sample_id[0]+ '_fbmn_' + ionization_mode + '_componentindex_' + str(component_index))
        g.add((s_feature_id, ns_kg.has_fbmn_ci, ci_node))
        g.add((t_feature_id, ns_kg.has_fbmn_ci, ci_node))
        
        link_node = rdflib.term.URIRef(kg_uri + 'lcms_feature_pair_' + usi_s + '_' + usi_t)
        g.add((link_node, RDF.type, ns_kg.LFpair))
        g.add((link_node, ns_kg.has_cosine, rdflib.term.Literal(cosine, datatype=XSD.float)))
        g.add((link_node, ns_kg.has_mass_difference, rdflib.term.Literal(mass_diff, datatype=XSD.float)))

        if graph_metadata[graph_metadata.feature_id == int(s)]['precursor_mz'].values[0] > graph_metadata[graph_metadata.feature_id == int(t)]['precursor_mz'].values[0]:
            g.add((link_node, ns_kg.has_member_1, s_feature_id))
            g.add((link_node, ns_kg.has_member_2, t_feature_id))
        else:
            g.add((link_node, ns_kg.has_member_1, t_feature_id))
            g.add((link_node, ns_kg.has_member_2, s_feature_id))

pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, f'indivual_mn_{ionization_mode}.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Results are in : {pathout}')  