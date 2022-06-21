import os
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF, RDFS
import sqlite3

# These lines allows to make sure that we are placed at the repo directory level 
from pathlib import Path

p = Path(__file__).parents[2]
os.chdir(p)

data_in_path = './data/in/vgf_unaligned_data_test/'
data_out_path = './data/in/'
path = os.path.normpath(data_in_path)

dat = sqlite3.connect(data_out_path+'/structures_metadata.db')
query = dat.execute("SELECT * From structures_metadata")
cols = [column[0] for column in query.description]
df_metadata = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)

g = Graph()
nm = g.namespace_manager

# Create jlw namespace
jlw_uri = "https://www.sinergiawolfender.org/jlw/"
ns_jlw = rdflib.Namespace(jlw_uri)
prefix = "mkg"
nm.bind(prefix, ns_jlw)

g.add((ns_jlw.InChIkey, RDFS.subClassOf, ns_jlw.ChemicalEntity))
g.add((ns_jlw.WDChemical, RDFS.subClassOf, ns_jlw.XRef))

for _, row in df_metadata.iterrows():
    short_ik = rdflib.term.URIRef(jlw_uri + row['short_inchikey'])
    g.add((short_ik, ns_jlw.has_smiles_2d, rdflib.term.Literal(row['smiles'])))
    g.add((short_ik, ns_jlw.has_np_pathway, rdflib.term.Literal(row['npc_pathway'])))
    g.add((short_ik, ns_jlw.has_np_superclass, rdflib.term.Literal(row['npc_superclass'])))
    g.add((short_ik, ns_jlw.has_np_class, rdflib.term.Literal(row['npc_class'])))
    if (row['wikidata_id'] != 'no_wikidata_match') & (row['wikidata_id'] != None):
        g.add((short_ik, ns_jlw.is_InChIkey2D_of, rdflib.term.URIRef(jlw_uri + row['inchikey'])))
        g.add((rdflib.term.URIRef(jlw_uri + row['inchikey']), ns_jlw.has_wd_id, rdflib.term.URIRef(row['wikidata_id'])))
        g.add((rdflib.term.URIRef(jlw_uri + row['inchikey']), RDF.type, ns_jlw.InChIkey))
        g.add((rdflib.term.URIRef(jlw_uri + row['inchikey']), ns_jlw.has_smiles, rdflib.term.Literal(row['isomeric_smiles'])))
        g.add((rdflib.term.URIRef(row['wikidata_id']), RDF.type, ns_jlw.WDChemical))
                
pathout = os.path.normpath("./data/out/")
os.makedirs(pathout, exist_ok=True)
g.serialize(destination="./data/out/structure_metadata.ttl", format="ttl", encoding="utf-8")        
