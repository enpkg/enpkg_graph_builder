import os
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDFS, RDF, XSD
import sqlite3
from pathlib import Path
 
p = Path(__file__).parents[2]
os.chdir(p)

g = Graph()
nm = g.namespace_manager

# Create jlw namespace
jlw_uri = "https://www.sinergiawolfender.org/jlw/"
ns_jlw = rdflib.Namespace(jlw_uri)
prefix = "mkg"
nm.bind(prefix, ns_jlw)

compound_chembl_url = 'https://www.ebi.ac.uk/chembl/compound_report_card/'
target_chembl_url = 'https://www.ebi.ac.uk/chembl/target_report_card/'
assay_chembl_url = 'https://www.ebi.ac.uk/chembl/assay_report_card/'
document_chembl_url = 'https://www.ebi.ac.uk/chembl/document_report_card/'

g.add((ns_jlw.WDChemical, RDFS.subClassOf, ns_jlw.XRef))
g.add((ns_jlw.ChEMBLChemical, RDFS.subClassOf, ns_jlw.XRef))
g.add((ns_jlw.ChEMBLTarget, RDFS.subClassOf, ns_jlw.XRef))
g.add((ns_jlw.ChEMBLDocument, RDFS.subClassOf, ns_jlw.XRef))
g.add((ns_jlw.ChEMBLAssay, RDFS.subClassOf, ns_jlw.XRef))
g.add((ns_jlw.ChEMBLAssayResults, RDFS.subClassOf, ns_jlw.XRef))

#path_bio = "C:/Users/gaudrya.FARMA/Github/biodereplication"
path_bio = "/Users/pma/Dropbox/git_repos/mandelbrot_project/biodereplication"
data_in_path = './data/in/vgf_unaligned_data_test/'

path_bio = os.path.normpath(path_bio)
path_bio = os.path.join(path_bio, 'data/out/chembl')
path = os.path.normpath(data_in_path)

metadata = []
for file in os.listdir(path_bio):
    df_bio_metadata = pd.read_csv(path_bio + '/' + file, index_col=0)
    metadata.append(df_bio_metadata)
df_bio_metadata = pd.concat(metadata)

dat = sqlite3.connect(path+'/structures_metadata.db')
query = dat.execute("SELECT * From structures_metadata")
cols = [column[0] for column in query.description]
df_metadata = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)

i = 1
for _, row in df_bio_metadata.iterrows():
    inchikey = row['inchikey']
    uri_ik = rdflib.term.URIRef(jlw_uri + inchikey)
    chembl_id_uri = rdflib.term.URIRef(compound_chembl_url + row['molecule_chembl_id'])
    target_id_uri = rdflib.term.URIRef(target_chembl_url + row['target_chembl_id'])
    assay_id_uri = rdflib.term.URIRef(assay_chembl_url + row['assay_chembl_id'])
    document_id_uri = rdflib.term.URIRef(document_chembl_url + row['document_chembl_id'])
    compound_activity_uri = rdflib.term.URIRef(jlw_uri + 'chembl_activity_' + str(i))
    i += 1
    
    g.add((rdflib.term.URIRef(jlw_uri + row['short_inchikey']), ns_jlw.is_InChIkey2D_of, uri_ik))
    g.add((rdflib.term.URIRef(jlw_uri + row['short_inchikey']), RDF.type, ns_jlw.InChIkey2D)) 
    g.add((uri_ik, ns_jlw.has_chembl_id, chembl_id_uri))    
    g.add((chembl_id_uri, ns_jlw.has_chembl_activity, compound_activity_uri))
    g.add((target_id_uri, ns_jlw.target_name, rdflib.term.Literal(row['target_pref_name'])))
    
    g.add((compound_activity_uri, ns_jlw.target_id, target_id_uri))
    g.add((compound_activity_uri, ns_jlw.assay_id, assay_id_uri))
    g.add((compound_activity_uri, ns_jlw.target_name, rdflib.term.Literal(row['target_pref_name'])))
    g.add((compound_activity_uri, ns_jlw.activity_type, rdflib.term.Literal(row['standard_type'])))
    g.add((compound_activity_uri, ns_jlw.activity_relation, rdflib.term.Literal(row['standard_relation'])))
    g.add((compound_activity_uri, ns_jlw.activity_value, rdflib.term.Literal(row['standard_value'], datatype=XSD.float)))
    g.add((compound_activity_uri, ns_jlw.activity_unit, rdflib.term.Literal(row['standard_units'])))
    g.add((compound_activity_uri, ns_jlw.stated_in_document, document_id_uri))
    g.add((document_id_uri, ns_jlw.journal_name, rdflib.term.Literal(row['document_journal'])))
    g.add((compound_activity_uri, RDFS.comment, rdflib.term.Literal(f"{row['standard_type']} of {row['molecule_chembl_id']} in assay {row['assay_chembl_id']} against {row['target_chembl_id']} ({row['target_pref_name']})")))

    g.add((target_id_uri, RDF.type, ns_jlw.ChEMBLTarget))
    g.add((chembl_id_uri, RDF.type, ns_jlw.ChEMBLChemical))
    g.add((document_id_uri, RDF.type, ns_jlw.ChEMBLDocument))
    g.add((assay_id_uri, RDF.type, ns_jlw.ChEMBLAssay))
    g.add((compound_activity_uri, RDF.type, ns_jlw.ChEMBLAssayResults))
    
    if (inchikey not in df_metadata['inchikey']):    
        g.add((uri_ik, ns_jlw.has_smiles, rdflib.term.Literal(row['isomeric_smiles'])))
        g.add((uri_ik, RDF.type, ns_jlw.InChIkey))
        if (row['wikidata_id'] != 'no_wikidata_match') & (row['wikidata_id'] != None):
            g.add((uri_ik, ns_jlw.has_wd_id, rdflib.term.URIRef(row['wikidata_id'])))
            g.add((rdflib.term.URIRef(row['wikidata_id']), RDF.type, ns_jlw.WDChemical))

pathout = os.path.normpath("./data/out/")
os.makedirs(pathout, exist_ok=True)
g.serialize(destination="./data/out/structure_biodereplication.ttl", format="ttl", encoding="utf-8")        
  