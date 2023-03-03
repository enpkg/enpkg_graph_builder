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

dat = sqlite3.connect(metadata_path)
query = dat.execute("SELECT * From structures_metadata")
cols = [column[0] for column in query.description]
df_metadata = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)

g = Graph()
nm = g.namespace_manager

# Create jlw namespace
kg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(kg_uri)
prefix = "enpkg"
nm.bind(prefix, ns_kg)

g.add((ns_kg.InChIkey, RDFS.subClassOf, ns_kg.ChemicalEntity))
g.add((ns_kg.WDChemical, RDFS.subClassOf, ns_kg.XRef))

for _, row in df_metadata.iterrows():
    short_ik = rdflib.term.URIRef(kg_uri + row['short_inchikey'])
    g.add((short_ik, ns_kg.has_smiles, rdflib.term.Literal(row['smiles'])))
    g.add((short_ik, ns_kg.has_np_pathway, rdflib.term.Literal(row['npc_pathway'])))
    g.add((short_ik, ns_kg.has_np_superclass, rdflib.term.Literal(row['npc_superclass'])))
    g.add((short_ik, ns_kg.has_np_class, rdflib.term.Literal(row['npc_class'])))
    if (row['wikidata_id'] != 'no_wikidata_match') & (row['wikidata_id'] != None):
        g.add((short_ik, ns_kg.is_InChIkey2D_of, rdflib.term.URIRef(kg_uri + row['inchikey'])))
        g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), ns_kg.has_wd_id, rdflib.term.URIRef(row['wikidata_id'])))
        g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), RDF.type, ns_kg.InChIkey))
        g.add((rdflib.term.URIRef(kg_uri + row['inchikey']), ns_kg.has_smiles, rdflib.term.Literal(row['isomeric_smiles'])))
        g.add((rdflib.term.URIRef(row['wikidata_id']), RDF.type, ns_kg.WDChemical))
                
pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, f'structures_metadata.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Result are in : {pathout}') 
