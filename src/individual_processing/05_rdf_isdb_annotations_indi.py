import os
import pandas as pd
import argparse
import textwrap
import rdflib
import json
from rdflib import Graph
from rdflib.namespace import RDF, RDFS, XSD
from tqdm import tqdm

# These lines allows to make sure that we are placed at the repo directory level 
from pathlib import Path


from pathlib import Path
p = Path(__file__).parents[2]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a RDF graph (.ttl format) from samples' individual ISDB annotations 
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



with open(os.path.normpath('data/adducts_formatter.json')) as json_file:
    adducts_dic = json.load(json_file)

path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in tqdm(samples_dir):
    
    g = Graph()
    nm = g.namespace_manager

    # Create jlw namespace
    kg_uri = "https://enpkg.commons-lab.org/kg/"
    ns_kg = rdflib.Namespace(kg_uri)
    prefix = "enpkg"
    nm.bind(prefix, ns_kg)

    isdb_path = os.path.join(path, directory, ionization_mode, 'isdb', directory + '_isdb_reweighted_flat_' + ionization_mode + '.tsv')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        isdb_annotations = pd.read_csv(isdb_path, sep='\t')
        metadata = pd.read_csv(metadata_path, sep='\t')
        isdb_annotations.adduct.fillna('[M+H]+', inplace=True)
        isdb_annotations.replace({"adduct": adducts_dic},inplace=True)
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    feature_count = []
    for _, row in isdb_annotations.iterrows():
        feature_count.append(row['feature_id'])
        count = feature_count.count(row['feature_id'])
        #feature_id = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_feature_" + str(row['feature_id']) + '_' + ionization_mode)
        InChIkey2D = rdflib.term.URIRef(kg_uri + row['short_inchikey'])
        #isdb_annotation_id = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_isdb_annotation_" + str(row['feature_id']) + '_' + ionization_mode + '_' + str(count))
        
        usi = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode+ '.mgf:scan:' + str(row['feature_id']) 
        feature_id = rdflib.term.URIRef(kg_uri + 'lcms_feature_' + usi)        
        isdb_annotation_id = rdflib.term.URIRef(kg_uri + "isdb_" + usi)
        
        g.add((feature_id, ns_kg.has_isdb_annotation, isdb_annotation_id))
        g.add((isdb_annotation_id, RDFS.label, rdflib.term.Literal(f"isdb annotation of {usi}")))
        g.add((isdb_annotation_id, ns_kg.has_InChIkey2D, InChIkey2D))
        g.add((isdb_annotation_id, ns_kg.has_spectral_score, rdflib.term.Literal(row['score_input'], datatype=XSD.float)))
        g.add((isdb_annotation_id, ns_kg.has_taxo_score, rdflib.term.Literal(row['score_taxo'], datatype=XSD.float)))
        g.add((isdb_annotation_id, ns_kg.has_consistency_score, rdflib.term.Literal(row['score_max_consistency'], datatype=XSD.float)))
        g.add((isdb_annotation_id, ns_kg.has_final_score, rdflib.term.Literal(row['final_score'], datatype=XSD.float)))        
        g.add((isdb_annotation_id, ns_kg.has_adduct, rdflib.term.Literal(row['adduct'])))
        g.add((InChIkey2D, RDF.type, ns_kg.InChIkey2D))
        g.add((isdb_annotation_id, RDF.type, ns_kg.IsdbAnnotation))
                 
    pathout = os.path.join(sample_dir_path, directory, "rdf/")
    os.makedirs(pathout, exist_ok=True)
    pathout = os.path.normpath(os.path.join(pathout, f'isdb_{ionization_mode}.ttl'))
    g.serialize(destination=pathout, format="ttl", encoding="utf-8")
    print(f'Results are in : {pathout}')     
