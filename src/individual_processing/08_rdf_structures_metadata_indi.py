import os
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF, RDFS
import sqlite3
import argparse
import textwrap

# These lines allows to make sure that we are placed at the repo directory level 
from pathlib import Path

p = Path(__file__).parents[2]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a RDF graph (.ttl format) from chemical structures metadata
         --------------------------------
            Arguments:
            - Path to the directory where samples folders are located
            - Path to the SQL metadata DB with compounds' metadata
        '''))

parser.add_argument('-p', '--sample_dir_path', required=True,
                    help='The path to the directory where samples folders to process are located')
parser.add_argument('-db', '--metadata_path', required=True,
                    help='The path to the structures metadata SQL DB')


args = parser.parse_args()
sample_dir_path = os.path.normpath(args.sample_dir_path)
metadata_path = os.path.normpath(args.metadata_path)

sample_dir_path = '/media/share/mapp_metabolomics_private/DBGI/ENPKG_DBGI_batch_tropical/indi_files'
metadata_path = '/home/allardpm/git_repos/ENPKG_DBGI/enpkg_meta_analysis/output_data/sql_db/structures_metadata.db'

dat = sqlite3.connect(metadata_path)
query = dat.execute("SELECT * From structures_metadata")
cols = [column[0] for column in query.description]
df_metadata = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)


g = Graph()
nm = g.namespace_manager

kg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(kg_uri)
prefix = "enpkg"
nm.bind(prefix, ns_kg)


path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []
for directory in tqdm(samples_dir):
    
    
for _, row in df_metadata.iterrows():
    
    print(_, row)
    
    short_ik = rdflib.term.URIRef(kg_uri + row['short_inchikey'])

    
    npc_pathway_list = row['npc_pathway'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").split('|')
    npc_superclass_list = row['npc_superclass'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").split('|')
    npc_class_list = row['npc_class'].replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").split('|')

    npc_pathway_urilist = []
    npc_superclass_urilist = []
    npc_class_urilist = []

    for list, uri_list in zip([npc_pathway_list, npc_superclass_list, npc_class_list],
                              [npc_pathway_urilist, npc_superclass_urilist, npc_class_urilist]):
        for item in list:
            uri_list.append(rdflib.term.URIRef(kg_uri + "npc_" + item))
                
    g.add((short_ik, ns_kg.has_smiles, rdflib.term.Literal(row['smiles'])))

    all_npc_pathway = []
    all_npc_superclass = []
    all_npc_class = []
    
    for uri in npc_pathway_urilist:
        g.add((short_ik, ns_kg.has_npc_pathway, uri))
        if uri not in all_npc_pathway:
            g.add((uri, RDF.type, ns_kg.NPCPathway))
            all_npc_pathway.append(uri)
    for uri in npc_superclass_urilist:
        g.add((short_ik, ns_kg.has_npc_superclass, uri))
        if uri not in all_npc_superclass:
            g.add((uri, RDF.type, ns_kg.NPCSuperclass))
            all_npc_superclass.append(uri)
    for uri in npc_class_urilist:
        g.add((short_ik, ns_kg.has_npc_class, uri))
        if uri not in all_npc_class:
            g.add((uri, RDF.type, ns_kg.NPCClass))
            all_npc_class.append(uri)
    
    if (row['wikidata_id'] != 'no_wikidata_match') & (row['wikidata_id'] != None):
        g.add((short_ik, ns_kg.is_InChIkey2D_of, rdflib.term.URIRef(kg_uri + row['inchikey'])))
        g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), ns_kg.has_wd_id, rdflib.term.URIRef(row['wikidata_id'])))
        g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), RDF.type, ns_kg.InChIkey))

        for uri in npc_pathway_urilist:
            g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), ns_kg.has_npc_pathway, uri))
        for uri in npc_superclass_urilist:
            g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), ns_kg.has_npc_superclass, uri))
        for uri in npc_class_urilist:
            g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), ns_kg.has_npc_class, uri))
            
        g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), ns_kg.has_smiles, rdflib.term.Literal(row['isomeric_smiles'])))
        g.add((rdflib.term.URIRef(row['wikidata_id']), RDF.type, ns_kg.WDChemical))
                
pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, f'structures_metadata.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Result are in : {pathout}') 
