import os
import pandas as pd
import rdflib
from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, XSD

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

g.add((ns_jlw.CanopusAnnotation, RDFS.subClassOf, ns_jlw.Annotation))

path = os.path.normpath(data_in_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in samples_dir:
    canopus_path = os.path.join(path, directory, directory + '_WORKSPACE_SIRIUS', 'npc_summary.csv')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        canopus_annotations = pd.read_csv(canopus_path, sep=',')
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    i = 1
    for _, row in canopus_annotations.iterrows():
        feature_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_feature_" + str(row['name']))
        canopus_annotation_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_canopus_annotation_" + str(i))
        i += 1
        g.add((feature_id, ns_jlw.has_sirius_annotation, canopus_annotation_id))
        g.add((canopus_annotation_id, RDFS.comment, rdflib.term.Literal('CANOPUS annotation')))
        g.add((canopus_annotation_id, ns_jlw.has_canopus_np_pathway, rdflib.term.Literal(row['pathway'])))
        g.add((canopus_annotation_id, ns_jlw.has_canopus_np_pathway_prob, rdflib.term.Literal(row['pathwayProbability'], datatype=XSD.float)))
        g.add((canopus_annotation_id, ns_jlw.has_canopus_np_superclass, rdflib.term.Literal(row['superclass'])))
        g.add((canopus_annotation_id, ns_jlw.has_canopus_np_superclass_prob, rdflib.term.Literal(row['superclassProbability'], datatype=XSD.float)))
        g.add((canopus_annotation_id, ns_jlw.has_canopus_np_class, rdflib.term.Literal(row['class'])))
        g.add((canopus_annotation_id, ns_jlw.has_canopus_np_class_prob, rdflib.term.Literal(row['classProbability'], datatype=XSD.float)))
        
pathout = os.path.normpath("./data/out/")
os.makedirs(pathout, exist_ok=True)
g.serialize(destination="./data/out/canopus.ttl", format="ttl", encoding="utf-8")