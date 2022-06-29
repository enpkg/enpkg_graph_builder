import os
import argparse
import textwrap
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF, RDFS, XSD

from pathlib import Path
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
for directory in samples_dir:
    quant_path = os.path.join(path, directory, ionization_mode, directory + '_features_quant_' + ionization_mode + '.csv')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        quant_table = pd.read_csv(quant_path, sep=',')
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    
    sample = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0])
    area_col = [col for col in quant_table.columns if col.endswith(' Peak area')][0]
        
    # Add feature list object to samples
    feature_list = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_MzMine_feature_list_" + ionization_mode)
    
    if ionization_mode == 'pos':
        g.add((sample, ns_jlw.has_MzMine_feature_list_pos, feature_list))
    elif ionization_mode == 'neg':
        g.add((sample, ns_jlw.has_MzMine_feature_list_neg, feature_list))
    
    g.add((feature_list, RDF.type, ns_jlw.MZmineChromatogram))
    g.add((feature_list, ns_jlw.has_ionization, rdflib.term.Literal(ionization_mode)))
    g.add((feature_list, RDFS.comment, rdflib.term.Literal(f"MzMine feature list in {ionization_mode} ionization mode of {metadata.sample_id[0]}")))
    # Add feature and their metadat to feature list
    for _, row in quant_table.iterrows():
        feature_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_feature_" + str(int(row['row ID'])) + '_' + ionization_mode) 
        g.add((feature_list, ns_jlw.has_MZmine_feature, feature_id))
        g.add((feature_id, ns_jlw.has_ionization, rdflib.term.Literal(ionization_mode)))
        g.add((feature_id, ns_jlw.has_row_id, rdflib.term.Literal(row['row ID'], datatype=XSD.integer)))
        g.add((feature_id, ns_jlw.has_parent_mass, rdflib.term.Literal(row['row m/z'], datatype=XSD.float)))
        g.add((feature_id, ns_jlw.has_retention_time, rdflib.term.Literal(row['row retention time'], datatype=XSD.float)))
        g.add((feature_id, ns_jlw.has_feature_area, rdflib.term.Literal(row[area_col])))
        g.add((feature_id, RDF.type, ns_jlw.MZmine_feature))

pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, f'features_{ionization_mode}.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Result are in : {pathout}')