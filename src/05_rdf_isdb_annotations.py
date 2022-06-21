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

g.add((ns_jlw.IsdbAnnotation, RDFS.subClassOf, ns_jlw.Annotation))

# ik2d_list = []
# # Load sirius annotation graph if it exists
# if os.path.isfile(os.path.join("../data/out/sirius.ttl")):
#     g_sirius = Graph().parse(os.path.join("../data/out/sirius.ttl"))
#     for s, p, o in g.triples((None, RDF.type, ns_jlw.chemical)):
#         ik2d_list.append(str(s)[-14:])      
                
path = os.path.normpath(data_in_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in samples_dir:
    isdb_path = os.path.join(path, directory, directory + '_isdb_matched_pos_repond_flat.tsv')
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        isdb_annotations = pd.read_csv(isdb_path, sep='\t')
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
    i = 1
    for _, row in isdb_annotations.iterrows():
        feature_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_feature_" + str(row['feature_id']))
        InChIkey2D = rdflib.term.URIRef(jlw_uri + row['short_inchikey'])
        isdb_annotation_id = rdflib.term.URIRef(jlw_uri + metadata.sample_id[0] + "_isdb_annotation_" + str(i))
        i += 1
        
        g.add((feature_id, ns_jlw.has_isdb_annotation, isdb_annotation_id))
        g.add((isdb_annotation_id, RDFS.comment, rdflib.term.Literal('ISDB annotation')))
        g.add((isdb_annotation_id, ns_jlw.has_InChIkey2D, InChIkey2D))
        #g.add((feature_id, ns_jlw.has_annotation, InChIkey2D))
        g.add((isdb_annotation_id, ns_jlw.has_spectral_score, rdflib.term.Literal(row['score_input'], datatype=XSD.float)))
        g.add((isdb_annotation_id, ns_jlw.has_taxo_score, rdflib.term.Literal(row['score_taxo'], datatype=XSD.float)))
        g.add((isdb_annotation_id, ns_jlw.has_consistency_score, rdflib.term.Literal(row['score_max_consistency'], datatype=XSD.float)))
        g.add((isdb_annotation_id, ns_jlw.has_final_score, rdflib.term.Literal(row['final_score'], datatype=XSD.float)))        
        g.add((isdb_annotation_id, ns_jlw.has_adduct, rdflib.term.Literal(row['adduct'])))
        g.add((InChIkey2D, RDF.type, ns_jlw.InChIkey2D))
        g.add((isdb_annotation_id, RDF.type, ns_jlw.IsdbAnnotation))
        # g.add((InChIkey2D, ns_jlw.has_ik, rdflib.term.Literal(row['structure_inchikey'])))  
        # g.add((InChIkey2D, ns_jlw.has_np_pathway, rdflib.term.Literal(row['structure_taxonomy_npclassifier_01pathway'])))
        # g.add((InChIkey2D, ns_jlw.has_np_superclass, rdflib.term.Literal(row['structure_taxonomy_npclassifier_02superclass'])))
        # g.add((InChIkey2D, ns_jlw.has_np_class, rdflib.term.Literal(row['structure_taxonomy_npclassifier_03class'])))
        # if InChIkey2D not in ik2d_list:
        #     g.add((InChIkey2D, RDF.type, ns_jlw.annotation))
            # g.add((InChIkey2D, ns_jlw.has_smiles, rdflib.term.Literal(row['structure_smiles'])))
            # g.add((InChIkey2D, ns_jlw.has_inchi, rdflib.term.Literal(row['structure_inchi'])))
            # g.add((InChIkey2D, ns_jlw.has_mf, rdflib.term.Literal(row['structure_molecular_formula']))) 
                 
pathout = os.path.normpath("./data/out/")
os.makedirs(pathout, exist_ok=True)
g.serialize(destination="./data/out/isdb.ttl", format="ttl", encoding="utf-8")        
