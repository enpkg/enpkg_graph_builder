import os
import argparse
import textwrap
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF, RDFS, XSD
from pathlib import Path
from tqdm import tqdm

p = Path(__file__).parents[1]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a RDF graph (.ttl format) from samples' individual Sirius annotations
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
kg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(kg_uri)
prefix = "enpkg"
nm.bind(prefix, ns_kg)

g.add((ns_kg.SiriusStructureAnnotation, RDFS.subClassOf, ns_kg.Annotation))
g.add((ns_kg.InChIkey2D, RDFS.subClassOf, ns_kg.ChemicalEntity))
g.add((ns_kg.Annotation, RDFS.comment, rdflib.term.Literal("A 2D structure that correspond to the annotation of at least 1 feature")))
g.add((ns_kg.InChIkey2D, RDFS.comment, rdflib.term.Literal("A 2D structure")))

path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in tqdm(samples_dir):
    csi_path = os.path.join(path, directory, ionization_mode, directory + '_WORKSPACE_SIRIUS', 'compound_identifications.tsv')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        csi_annotations = pd.read_csv(csi_path, sep='\t')
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    for _, row in csi_annotations.iterrows():
        feature_id_int = row['id'].rsplit('_', 1)[1]
        # feature_id = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_feature_" + str(feature_id_int) + '_' + ionization_mode)
        # sirius_annotation_id = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_sirius_annotation_" + str(feature_id_int)  + '_' + ionization_mode)
        
        usi = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode+ '.mgf:scan:' + str(int(feature_id_int))
        feature_id = rdflib.term.URIRef(kg_uri + 'lcms_feature_' + usi)
        sirius_annotation_id = rdflib.term.URIRef(kg_uri + "sirius_" + usi)
        
        InChIkey2D = rdflib.term.URIRef(kg_uri + row['InChIkey2D'])
        
        g.add((feature_id, ns_kg.has_sirius_annotation, sirius_annotation_id))
        g.add((sirius_annotation_id, ns_kg.has_InChIkey2D, InChIkey2D))
        g.add((sirius_annotation_id, ns_kg.has_ionization, rdflib.term.Literal(ionization_mode)))
        g.add((sirius_annotation_id, RDFS.label, rdflib.term.Literal(f"sirius annotation of {usi}")))
        #g.add((feature_id, ns_kg.has_annotation, InChIkey2D))
        g.add((sirius_annotation_id, ns_kg.has_sirius_adduct, rdflib.term.Literal(row['adduct'])))
        g.add((sirius_annotation_id, ns_kg.has_sirius_score, rdflib.term.Literal(row['SiriusScore'], datatype=XSD.float)))
        g.add((sirius_annotation_id, ns_kg.has_zodiac_score, rdflib.term.Literal(row['ZodiacScore'], datatype=XSD.float)))
        g.add((sirius_annotation_id, ns_kg.has_cosmic_score, rdflib.term.Literal(row['ConfidenceScore'], datatype=XSD.float)))       
        g.add((InChIkey2D, RDF.type, ns_kg.InChIkey2D))
        g.add((sirius_annotation_id, RDF.type, ns_kg.SiriusStructureAnnotation))

        
pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, f'sirius_{ionization_mode}.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Results are in : {pathout}')     