import os
import pandas as pd
import rdflib
from rdflib import Graph
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

g.add((ns_jlw.SiriusAnnotation, RDFS.subClassOf, ns_jlw.Annotation))
g.add((ns_jlw.InChIkey2D, RDFS.subClassOf, ns_jlw.ChemicalEntity))
g.add((ns_jlw.Annotation, RDFS.comment, rdflib.term.Literal("A 2D structure that correspond to the annotation of at least 1 feature")))
g.add((ns_jlw.InChIkey2D, RDFS.comment, rdflib.term.Literal("A 2D structure")))

path = os.path.normpath(data_in_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in samples_dir:
    csi_path = os.path.join(path, directory, directory + '_WORKSPACE_SIRIUS', 'compound_identifications.tsv')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        csi_annotations = pd.read_csv(csi_path, sep='\t')
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    i = 1
    for _, row in csi_annotations.iterrows():
        feature_id = row['id'].rsplit('_', 1)[1]
        feature_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_feature_" + str(feature_id))
        sirius_annotation_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_sirius_annotation_" + str(i))
        InChIkey2D = rdflib.term.URIRef(jlw_uri + row['InChIkey2D'])
        i += 1
        
        g.add((feature_id, ns_jlw.has_sirius_annotation, sirius_annotation_id))
        g.add((sirius_annotation_id, ns_jlw.has_InChIkey2D, InChIkey2D))
        g.add((sirius_annotation_id, RDFS.comment, rdflib.term.Literal('Sirius annotation')))
        #g.add((feature_id, ns_jlw.has_annotation, InChIkey2D))
        g.add((sirius_annotation_id, ns_jlw.has_sirius_adduct, rdflib.term.Literal(row['adduct'])))
        g.add((sirius_annotation_id, ns_jlw.has_sirius_score, rdflib.term.Literal(row['SiriusScore'], datatype=XSD.float)))
        g.add((sirius_annotation_id, ns_jlw.has_zodiac_score, rdflib.term.Literal(row['ZodiacScore'], datatype=XSD.float)))
        g.add((sirius_annotation_id, ns_jlw.has_cosmic_score, rdflib.term.Literal(row['ConfidenceScore'], datatype=XSD.float)))       
        g.add((InChIkey2D, RDF.type, ns_jlw.InChIkey2D))
        g.add((sirius_annotation_id, RDF.type, ns_jlw.SiriusAnnotation))

        
pathout = os.path.normpath("./data/out/")
os.makedirs(pathout, exist_ok=True)
g.serialize(destination="./data/out/sirius.ttl", format="ttl", encoding="utf-8")        