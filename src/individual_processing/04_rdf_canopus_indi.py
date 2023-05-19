import os
import yaml
import argparse
import textwrap
import pandas as pd
import rdflib
from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, XSD
from pathlib import Path
from tqdm import tqdm

p = Path(__file__).parents[2]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a RDF graph (.ttl format) from samples' individual CANOPUS annotations 
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


greek_alphabet = 'ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩωÎ²Iµ'
latin_alphabet = 'AaBbGgDdEeZzHhJjIiKkLlMmNnXxOoPpRrSssTtUuFfQqYyWwI2Iu'
greek2latin = str.maketrans(greek_alphabet, latin_alphabet)


path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in tqdm(samples_dir):
    g = Graph()
    nm = g.namespace_manager
    kg_uri = "https://enpkg.commons-lab.org/kg/"
    ns_kg = rdflib.Namespace(kg_uri)
    prefix = "enpkg"
    nm.bind(prefix, ns_kg)
        
    sirius_param_path = os.path.join(path, directory, ionization_mode, directory + '_WORKSPACE_SIRIUS', 'params.yml')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:   
        open(sirius_param_path)
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    try:
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue

    
    with open (sirius_param_path) as file:    
        params_list = yaml.load(file, Loader=yaml.FullLoader)
    sirius_version = params_list['options'][0]['sirius_version']
    
    if sirius_version == 4:
        # Canopus NPC results integration for sirius 4
        try:
            canopus_npc_path = os.path.join(path, directory, ionization_mode, directory + '_WORKSPACE_SIRIUS', 'npc_summary.csv')
            canopus_annotations = pd.read_csv(canopus_npc_path)
            canopus_annotations.fillna('Unknown', inplace=True)
            for _, row in canopus_annotations.iterrows():        
                # feature_id = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_feature_" + str(row['name']) + '_' + ionization_mode)
                # canopus_annotation_id = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_canopus_annotation_" + str(row['name'])+ '_' + ionization_mode)
                
                usi = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode+ '.mgf:scan:' + str(row['name'])
                feature_id = rdflib.term.URIRef(kg_uri + 'lcms_feature_' + usi)
                canopus_annotation_id = rdflib.term.URIRef(kg_uri + "canopus_" + usi)
                
                npc_pathway = rdflib.term.URIRef(kg_uri + "npc_" + row['pathway'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").translate(greek2latin))
                npc_superclass = rdflib.term.URIRef(kg_uri + "npc_" + row['superclass'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").translate(greek2latin))
                npc_class = rdflib.term.URIRef(kg_uri + "npc_" + row['class'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").translate(greek2latin))
                
                g.add((feature_id, ns_kg.has_canopus_annotation, canopus_annotation_id))
                g.add((canopus_annotation_id, RDFS.label, rdflib.term.Literal(f"canopus annotation of {usi}")))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_pathway, npc_pathway))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_pathway_prob, rdflib.term.Literal(row['pathwayProbability'], datatype=XSD.float)))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_superclass, npc_superclass))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_superclass_prob, rdflib.term.Literal(row['superclassProbability'], datatype=XSD.float)))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_class, npc_class))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_class_prob, rdflib.term.Literal(row['classProbability'], datatype=XSD.float)))
                g.add((canopus_annotation_id, RDF.type, ns_kg.SiriusCanopusAnnotation))
        except FileNotFoundError:
            pass
        except NotADirectoryError:
            continue
        
    elif sirius_version == 5:
        # Canopus NPC results integration for sirius 5
        try:
            canopus_npc_path = os.path.join(path, directory, ionization_mode, directory + '_WORKSPACE_SIRIUS', 'canopus_compound_summary.tsv')
            canopus_annotations = pd.read_csv(canopus_npc_path, sep='\t')
            canopus_annotations.fillna('Unknown', inplace=True)
            for _, row in canopus_annotations.iterrows():
                
                feature_id = row['id'].rsplit('_', 1)[1]
                # canopus_annotation_id = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_canopus_annotation_" + str(feature_id)+ '_' + ionization_mode)                
                # feature_id = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_feature_" + str(feature_id)+ '_' + ionization_mode)             
                usi = 'mzspec:' + metadata['massive_id'][0] + ':' + metadata.sample_id[0] + '_features_ms2_'+ ionization_mode+ '.mgf:scan:' + str(feature_id)
                feature_id = rdflib.term.URIRef(kg_uri + 'lcms_feature_' + usi)
                canopus_annotation_id = rdflib.term.URIRef(kg_uri + "canopus_" + usi)
                
                npc_pathway = rdflib.term.URIRef(kg_uri + "npc_" + row['NPC#pathway'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").translate(greek2latin))
                npc_superclass = rdflib.term.URIRef(kg_uri + "npc_" + row['NPC#superclass'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").translate(greek2latin))
                npc_class = rdflib.term.URIRef(kg_uri + "npc_" + row['NPC#class'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").translate(greek2latin))
                    
                g.add((feature_id, ns_kg.has_canopus_annotation, canopus_annotation_id))
                g.add((canopus_annotation_id, RDFS.label, rdflib.term.Literal(f"canopus annotation of {usi}")))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_pathway, npc_pathway))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_pathway_prob, rdflib.term.Literal(row['NPC#pathway Probability'], datatype=XSD.float)))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_superclass, npc_superclass))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_superclass_prob, rdflib.term.Literal(row['NPC#superclass Probability'], datatype=XSD.float)))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_class, npc_class))
                g.add((canopus_annotation_id, ns_kg.has_canopus_npc_class_prob, rdflib.term.Literal(row['NPC#class Probability'], datatype=XSD.float)))
                g.add((canopus_annotation_id, RDF.type, ns_kg.SiriusCanopusAnnotation))
        except FileNotFoundError:
            pass
        except NotADirectoryError:
            continue
        
    pathout = os.path.join(sample_dir_path, directory, "rdf/")
    os.makedirs(pathout, exist_ok=True)
    pathout = os.path.normpath(os.path.join(pathout, f'canopus_{ionization_mode}.ttl'))
    g.serialize(destination=pathout, format="ttl", encoding="utf-8")
    print(f'Results are in : {pathout}')   