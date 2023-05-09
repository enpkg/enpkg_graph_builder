import os
import argparse
import textwrap
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF, RDFS, XSD, FOAF
from tqdm import tqdm

from pathlib import Path
p = Path(__file__).parents[2]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a RDF graph (.ttl format) from samples' individual features files 
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

path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]
for directory in tqdm(samples_dir):

    quant_path = os.path.join(path, directory, ionization_mode, directory + '_features_quant_' + ionization_mode + '.csv')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        quant_table = pd.read_csv(quant_path, sep=',')
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    
    if metadata.sample_type[0] == 'sample':
        g = Graph()
        nm = g.namespace_manager
        nm.bind(prefix, ns_kg)

        sample = rdflib.term.URIRef(kg_uri + metadata.sample_id[0])
        area_col = [col for col in quant_table.columns if col.endswith(' Peak area')][0]
        max_area = quant_table[area_col].max()
            
        # Add feature list object to samples
        feature_list = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_lcms_feature_list_" + ionization_mode)

        if ionization_mode == 'pos':
            lc_ms = rdflib.term.URIRef(kg_uri + metadata['sample_filename_pos'][0])
        elif ionization_mode == 'neg':
            lc_ms = rdflib.term.URIRef(kg_uri + metadata['sample_filename_neg'][0])
        
        g.add((lc_ms, ns_kg.has_lcms_feature_list, feature_list))

        g.add((feature_list, RDF.type, ns_kg.LCMSFeatureList))
        g.add((feature_list, ns_kg.has_ionization, rdflib.term.Literal(ionization_mode)))
        g.add((feature_list, RDFS.comment, rdflib.term.Literal(f"LCMS feature list in {ionization_mode} ionization mode of {metadata.sample_id[0]}")))
        # Add feature and their metadat to feature list
        for _, row in quant_table.iterrows():
            usi = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode+ '.mgf:scan:' + str(int(row['row ID']))
            feature_id = rdflib.term.URIRef(kg_uri + 'lcms_feature_' + usi)
            g.add((feature_list, ns_kg.has_lcms_feature, feature_id))
            g.add((feature_id, RDF.type, ns_kg.LCMSFeature))
            g.add((feature_id, RDFS.label, rdflib.term.Literal(f"lcms_feature {usi}")))
            g.add((feature_id, ns_kg.has_ionization, rdflib.term.Literal(ionization_mode)))
            g.add((feature_id, ns_kg.has_row_id, rdflib.term.Literal(row['row ID'], datatype=XSD.integer)))
            g.add((feature_id, ns_kg.has_parent_mass, rdflib.term.Literal(row['row m/z'], datatype=XSD.float)))
            g.add((feature_id, ns_kg.has_retention_time, rdflib.term.Literal(row['row retention time'], datatype=XSD.float)))
            g.add((feature_id, ns_kg.has_feature_area, rdflib.term.Literal(row[area_col], datatype=XSD.float)))
            g.add((feature_id, ns_kg.has_relative_feature_area, rdflib.term.Literal(row[area_col]/max_area, datatype=XSD.float)))
            
            g.add((feature_id, ns_kg.has_usi, rdflib.term.Literal(usi)))
            link_spectrum = 'https://metabolomics-usi.ucsd.edu/dashinterface/?usi1=' + usi
            g.add((feature_id, ns_kg.gnps_dashboard_view, rdflib.URIRef(link_spectrum)))
            link_png = 'https://metabolomics-usi.ucsd.edu/png/?usi1=' + usi
            g.add((feature_id, FOAF.depiction, rdflib.URIRef(link_png))) 
            
        pathout = os.path.join(sample_dir_path, directory, "rdf/")
        os.makedirs(pathout, exist_ok=True)
        pathout = os.path.normpath(os.path.join(pathout, f'features_{ionization_mode}.ttl'))
        g.serialize(destination=pathout, format="ttl", encoding="utf-8")
        print(f'Results are in : {pathout}')