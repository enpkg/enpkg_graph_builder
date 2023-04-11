import os
import argparse
import textwrap
import pandas as pd
import rdflib
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, XSD
from pathlib import Path
from tqdm import tqdm

p = Path(__file__).parents[1]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a RDF graph (.ttl format) from samples' metadata 
         --------------------------------
            Arguments:
            - Path to the directory where samples folders are located
        '''))

parser.add_argument('-p', '--sample_dir_path', required=True,
                    help='The path to the directory where samples folders to process are located')

args = parser.parse_args()
sample_dir_path = os.path.normpath(args.sample_dir_path)

g = Graph()
nm = g.namespace_manager

WD = Namespace('http://www.wikidata.org/entity/')
g.namespace_manager.bind('wd', WD)

# Create enpkg namespace
kg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(kg_uri)
prefix = "enpkg"
nm.bind(prefix, ns_kg)

# Create enpkgdemo namespace
demo_uri = "https://enpkg.commons-lab.org/module/"
ns_demo = rdflib.Namespace(demo_uri)
prefix = "enpkgmodule"
nm.bind(prefix, ns_demo)

target_chembl_url = 'https://www.ebi.ac.uk/chembl/target_report_card/'
g.add((ns_demo.ChEMBLTarget, RDFS.subClassOf, ns_kg.XRef))

path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]

# We define a lab process entity 
g.add((ns_kg.BioAssayResults, RDF.type, RDF.Property))
g.add((ns_demo.SwissTPHBioAssay, RDFS.subClassOf, ns_kg.BioAssayResults))

g.add((ns_demo.Ldono10ugml, RDFS.subClassOf, ns_demo.SwissTPHBioAssay))
g.add((ns_demo.Ldono2ugml, RDFS.subClassOf, ns_demo.SwissTPHBioAssay))
g.add((ns_demo.Tbrucei10ugml, RDFS.subClassOf, ns_demo.SwissTPHBioAssay))
g.add((ns_demo.Tbrucei2ugml, RDFS.subClassOf, ns_demo.SwissTPHBioAssay))
g.add((ns_demo.Tcruzi10ugml, RDFS.subClassOf, ns_demo.SwissTPHBioAssay))
g.add((ns_demo.L610ugml, RDFS.subClassOf, ns_demo.SwissTPHBioAssay))

for directory in tqdm(samples_dir):    
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
        
    sample = rdflib.term.URIRef(kg_uri + metadata.sample_id[0])
    
    if metadata.sample_type[0] == 'sample':
        material_id = rdflib.term.URIRef(kg_uri + metadata.sample_substance_name[0])
        plant_parts = metadata[['organism_organe', 'organism_broad_organe', 'organism_tissue', 'organism_subsystem']].copy()
        plant_parts.fillna('unkown', inplace=True)
        plant_parts.replace(' ', '_', regex=True, inplace=True)
        
        g.add((material_id, ns_demo.has_organe, rdflib.term.URIRef(demo_uri + plant_parts['organism_organe'][0])))
        g.add((material_id, ns_demo.has_broad_organe, rdflib.term.URIRef(demo_uri + plant_parts['organism_broad_organe'][0])))
        g.add((material_id, ns_demo.has_tissue, rdflib.term.URIRef(demo_uri + plant_parts['organism_tissue'][0])))
        g.add((material_id, ns_demo.has_subsystem, rdflib.term.URIRef(demo_uri + plant_parts['organism_subsystem'][0])))
                
        
        for assay_id, target, chembl_id, rdfclass in zip(
            ['bio_leish_donovani_10ugml_inhibition', 'bio_leish_donovani_2ugml_inhibition', 'bio_tryp_brucei_rhodesiense_10ugml_inhibition', \
            'bio_tryp_brucei_rhodesiense_2ugml_inhibition', 'bio_tryp_cruzi_10ugml_inhibition', 'bio_l6_cytotoxicity_10ugml_inhibition'], 
            ['Ldonovani_10ugml', 'Ldonovani_2ugml', 'Tbruceirhod_10ugml', 'Tbruceirhod_2ugml', 'Tcruzi_10ugml', 'L6_10ugml'],
            ['CHEMBL367', 'CHEMBL367', 'CHEMBL612348', 'CHEMBL612348', 'CHEMBL368', None],
            [ns_demo.Ldono10ugml, ns_demo.Ldono2ugml, ns_demo.Tbrucei10ugml, ns_demo.Tbrucei2ugml, ns_demo.Tcruzi10ugml, ns_demo.L610ugml]):    
                   
                assay = rdflib.term.URIRef(demo_uri + metadata.sample_id[0] + "_" + target)
                type = rdflib.term.URIRef(demo_uri + target)
                g.add((sample, ns_demo.has_bioassay_results, assay))
                g.add((assay, RDFS.label, rdflib.term.Literal(f"{target} assay of {metadata.sample_id[0]}")))
                g.add((assay, ns_demo.inhibition_percentage, rdflib.term.Literal(metadata[assay_id][0], datatype=XSD.float)))
                g.add((assay, RDF.type, rdfclass))
                if chembl_id is not None:
                    target_id_uri = rdflib.term.URIRef(target_chembl_url + chembl_id)
                    g.add((assay, ns_demo.target_id, target_id_uri))
                    g.add((target_id_uri, RDF.type, ns_demo.ChEMBLTarget))             
        
pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, 'metadata_demo.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Results are in : {pathout}')