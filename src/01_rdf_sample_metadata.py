import os
import argparse
import textwrap
import pandas as pd
import rdflib
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, XSD, FOAF
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


EX = Namespace('http://EXample.org/')
RL = Namespace('http://purl.org/vocab/relationship/')
DBO = Namespace('https://dbpedia.org/ontology/')
DBR = Namespace('https://dbpedia.org/page/')
WD = Namespace('http://www.wikidata.org/entity/')


g.namespace_manager.bind('exampleURI', EX)
g.namespace_manager.bind('relationship', RL)
g.namespace_manager.bind('dbpediaOntology', DBO)
g.namespace_manager.bind('dbpediaPage', DBR)
g.namespace_manager.bind('wd', WD)

# Create jlw namespace
kg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(kg_uri)
prefix = "enpkg"
nm.bind(prefix, ns_kg)

target_chembl_url = 'https://www.ebi.ac.uk/chembl/target_report_card/'
g.add((ns_kg.ChEMBLTarget, RDFS.subClassOf, ns_kg.XRef))


path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []


# We define a lab process entity 
g.add((ns_kg.LabProcess, RDF.type, RDF.Property))
g.add((ns_kg.LabProcess, RDFS.label, rdflib.term.Literal("A lab process")))
g.add((ns_kg.has_lab_process, RDF.type, ns_kg.LabProcess))
g.add((ns_kg.WDTaxon, RDFS.subClassOf, ns_kg.XRef))
g.add((ns_kg.BioAssayResults, RDF.type, RDF.Property))
g.add((ns_kg.SwissTPHBioAssay, RDFS.subClassOf, ns_kg.BioAssayResults))

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

        # We define a pf_code entity form the metadata 'sample_substance_name'. And remove the last 5 characters
        # this can be optionally adapted to fit your metadata
        pf_code = rdflib.term.URIRef(kg_uri + metadata.sample_substance_name[0][:-5])
        #pf_code = rdflib.term.URIRef(kg_uri + metadata.sample_id[0])       
        g.add((pf_code, RDF.type, ns_kg.PFCode))
        g.add((pf_code, ns_kg.has_lab_process, sample))
        g.add((sample, RDF.type, ns_kg.LabExtract))
        g.add((sample, ns_kg.type, ns_kg.LabExtract))
        g.add((sample, RDFS.comment, rdflib.term.Literal(f"Extract {metadata.sample_id[0]}")))
        
        plant_parts = metadata[['organism_organe', 'organism_broad_organe', 'organism_tissue', 'organism_subsystem']].copy()
        plant_parts.fillna('unkown', inplace=True)
        plant_parts.replace(' ', '_', regex=True, inplace=True)
        
        g.add((pf_code, ns_kg.has_organe, rdflib.term.URIRef(kg_uri + plant_parts['organism_organe'][0])))
        g.add((pf_code, ns_kg.has_broad_organe, rdflib.term.URIRef(kg_uri + plant_parts['organism_broad_organe'][0])))
        g.add((pf_code, ns_kg.has_tissue, rdflib.term.URIRef(kg_uri + plant_parts['organism_tissue'][0])))
        g.add((pf_code, ns_kg.has_subsystem, rdflib.term.URIRef(kg_uri + plant_parts['organism_subsystem'][0])))
        
        # Add GNPS Dashborad link for pos & neg: only if sample_filename_pos column exists and is not NaN and MassIVE id is present
        if set(['sample_filename_pos', 'massive_id']).issubset(metadata.columns):
            if not pd.isna(metadata['sample_filename_pos'][0]):
                sample_filename_pos = metadata['sample_filename_pos'][0]
                massive_id = metadata['massive_id'][0]    
                gnps_dashboard_link = f'https://gnps-lcms.ucsd.edu/?usi=mzspec:{massive_id}:{sample_filename_pos}'
                gnps_tic_pic = f'https://gnps-lcms.ucsd.edu/mspreview?usi=mzspec:{massive_id}:{sample_filename_pos}'
                g.add((sample, ns_kg.has_LCMS_pos, rdflib.term.URIRef(kg_uri + metadata['sample_filename_pos'][0])))
                g.add((rdflib.term.URIRef(kg_uri + metadata['sample_filename_pos'][0]), ns_kg.has_gnpslcms_link_pos, rdflib.URIRef(gnps_dashboard_link)))
                g.add((rdflib.term.URIRef(kg_uri + metadata['sample_filename_pos'][0]), FOAF.depiction, rdflib.URIRef(gnps_tic_pic))) 
                
        if set(['sample_filename_neg', 'massive_id']).issubset(metadata.columns):
            if not pd.isna(metadata['sample_filename_neg'][0]):
                sample_filename_neg = metadata['sample_filename_neg'][0]
                massive_id = metadata['massive_id'][0]    
                gnps_dashboard_link = f'https://gnps-lcms.ucsd.edu/?usi=mzspec:{massive_id}:{sample_filename_neg}'
                gnps_tic_pic = f'https://gnps-lcms.ucsd.edu/mspreview?usi=mzspec:{massive_id}:{sample_filename_neg}'
                g.add((sample, ns_kg.has_LCMS_neg, rdflib.term.URIRef(kg_uri + metadata['sample_filename_neg'][0])))
                g.add((rdflib.term.URIRef(kg_uri + metadata['sample_filename_neg'][0]), ns_kg.has_gnpslcms_link_neg, rdflib.URIRef(gnps_dashboard_link)))
                g.add((rdflib.term.URIRef(kg_uri + metadata['sample_filename_neg'][0]), FOAF.depiction, rdflib.URIRef(gnps_tic_pic))) 
                
        
        for assay_id, target, chembl_id in zip(
            ['bio_leish_donovani_10ugml_inhibition', 'bio_leish_donovani_2ugml_inhibition', 'bio_tryp_brucei_rhodesiense_10ugml_inhibition', \
            'bio_tryp_brucei_rhodesiense_2ugml_inhibition', 'bio_tryp_cruzi_10ugml_inhibition', 'bio_l6_cytotoxicity_10ugml_inhibition'], 
            ['Ldonovani_10ugml', 'Ldonovani_2ugml', 'Tbruceirhod_10ugml', 'Tbruceirhod_2ugml', 'Tcruzi_10ugml', 'L6_10ugml'],
            ['CHEMBL367', 'CHEMBL367', 'CHEMBL612348', 'CHEMBL612348', 'CHEMBL368', None]):            
                assay = rdflib.term.URIRef(kg_uri + metadata.sample_id[0] + "_" + target)
                type = rdflib.term.URIRef(kg_uri + target)
                g.add((sample, ns_kg.has_bioassay_results, assay))
                g.add((assay, RDFS.label, rdflib.term.Literal(f"{target} assay of {metadata.sample_id[0]}")))
                g.add((assay, ns_kg.inhibition_percentage, rdflib.term.Literal(metadata[assay_id][0], datatype=XSD.float)))
                g.add((assay, RDF.type, ns_kg.SwissTPHBioAssay))
                if chembl_id is not None:
                    target_id_uri = rdflib.term.URIRef(target_chembl_url + chembl_id)
                    g.add((assay, ns_kg.target_id, target_id_uri))
                    g.add((target_id_uri, RDF.type, ns_kg.ChEMBLTarget))             
        
        # Add WD taxonomy link to substance
        metadata_taxo_path = os.path.join(path, directory, 'taxo_output', directory + '_taxo_metadata.tsv')
        try:
            metadata_taxo = pd.read_csv(metadata_taxo_path, sep='\t')          
            if not pd.isna(metadata_taxo['wd.value'][0]):
                wd_id = rdflib.term.URIRef(WD + metadata_taxo['wd.value'][0][31:])
                g.add((pf_code, ns_kg.has_wd_id, wd_id))
                g.add((wd_id, RDF.type, ns_kg.WDTaxon))
            else:
                g.add((pf_code, ns_kg.has_unresolved_taxon, rdflib.term.URIRef(kg_uri + 'unresolved_taxon')))              
        except FileNotFoundError:
            g.add((pf_code, ns_kg.has_unresolved_taxon, rdflib.term.URIRef(kg_uri + 'unresolved_taxon'))) 
            pass 
              
    elif metadata.sample_type[0] == 'blank':        
        g.add((sample, RDF.type, ns_kg.LabBlank))
        g.add((sample, RDFS.comment, rdflib.term.Literal(f"Blank {metadata.sample_id[0]}")))
    elif metadata.sample_type[0] == 'qc':        
        g.add((sample, RDF.type, ns_kg.LabQc))
        g.add((sample, RDFS.comment, rdflib.term.Literal(f"QC {metadata.sample_id[0]}")))

pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, 'metadata.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Results are in : {pathout}')