from rdflib import Graph, Namespace
from pathlib import Path
import os
from tqdm import tqdm
import rdflib
import argparse
import textwrap

# These lines allows to make sure that we are placed at the repo directory level 
p = Path(__file__).parents[2]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a unique RDF graph by sample (.ttl format) from multiples sample specific .rdf files.
         --------------------------------
            Arguments:
            - Path to the directory where samples folders are located
        '''))

parser.add_argument('-p', '--sample_dir_path', required=True,
                    help='The path to the directory where samples folders to process are located')

args = parser.parse_args()
sample_dir_path = os.path.normpath(args.sample_dir_path)

"""
sample_dir_path = '/Users/arnaudgaudry/Desktop/VGF_data_organized'
metadata_path = '/Users/arnaudgaudry/Github/enpkg_meta_analysis/output_data/sql_db/structures_metadata.db'
"""

# Create enpkg namespace
kg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(kg_uri)
prefix_kg = "enpkg"

# Create enpkgmodule namespace
module_uri = "https://enpkg.commons-lab.org/module/"
ns_module = rdflib.Namespace(module_uri)
prefix_module = "enpkgmodule"
WD = Namespace('http://www.wikidata.org/entity/')

path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in tqdm(samples_dir):
    merged_graph = Graph()
    nm = merged_graph.namespace_manager
    nm.bind(prefix_kg, ns_kg)
    nm.bind(prefix_module, ns_module)
    nm.bind("wd", WD)

    # Iterate over the files and add their contents to the merged graph
    exist_files = []
    for file_path in ["rdf/canopus_pos.ttl", "rdf/canopus_neg.ttl", 
                      "rdf/features_pos.ttl", "rdf/features_neg.ttl", 
                      "rdf/features_spe2vec_pos.ttl", "rdf/features_spe2vec_neg.ttl",
                      "rdf/indivual_mn_pos.ttl", "rdf/indivual_mn_neg.ttl",
                      "rdf/isdb_pos.ttl", "rdf/isdb_neg.ttl",
                      "rdf/sirius_pos.ttl", "rdf/sirius_neg.ttl"
                      "rdf/metadata_enpkg.ttl", "rdf/metadata_module_enpkg.ttl", "rdf/structures_metadata.ttl"]:
        if os.path.isfile(os.path.join(sample_dir_path, directory, file_path)):
            exist_files.append(os.path.join(sample_dir_path, directory, file_path))
    if len(exist_files) > 0:
        for file_path in exist_files:
            with open(file_path, "r") as f:
                file_content = f.read()
                merged_graph.parse(data=file_content, format="ttl")
    else:
        continue

    pathout = os.path.join(sample_dir_path, directory, "rdf/")
    os.makedirs(pathout, exist_ok=True)
    pathout = os.path.normpath(os.path.join(pathout, f'{directory}_merged_graph.ttl'))
    merged_graph.serialize(destination=pathout, format="ttl", encoding="utf-8")
    print(f'Results are in : {pathout}') 
