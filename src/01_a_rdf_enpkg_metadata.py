import os
from pathlib import Path
import argparse
import textwrap
import pandas as pd
import rdflib
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, FOAF
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
nm.bind('wd', WD)

# Create enpkg namespace
enpkg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(enpkg_uri)
prefix = "enpkg"
nm.bind(prefix, ns_kg)

path = os.path.normpath(sample_dir_path)
samples_dir = [directory for directory in os.listdir(path)]

# Define a lab process entity 
g.add((ns_kg.LabProcess, RDF.type, RDF.Property))
g.add((ns_kg.has_lab_process, RDF.type, ns_kg.LabProcess))
g.add((ns_kg.WDTaxon, RDFS.subClassOf, ns_kg.XRef))

for directory in tqdm(samples_dir):    
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
        continue
        
    sample = rdflib.term.URIRef(enpkg_uri + metadata.sample_id[0])
    
    if metadata.sample_type[0] == 'sample':
        material_id = rdflib.term.URIRef(enpkg_uri + metadata.sample_substance_name[0])
        g.add((material_id, RDF.type, ns_kg.MaterialID))
        g.add((material_id, ns_kg.submitted_taxon, rdflib.term.Literal(metadata.organism_species[0])))
        g.add((material_id, ns_kg.has_lab_process, sample))
        g.add((sample, RDF.type, ns_kg.LabProcess))
        g.add((sample, RDFS.label, rdflib.term.Literal(f"Sample {metadata.sample_id[0]}")))
        
        # Add GNPS Dashborad link for pos & neg: only if sample_filename_pos column exists and is not NaN and MassIVE id is present
        if set(['sample_filename_pos', 'massive_id']).issubset(metadata.columns):
            if not pd.isna(metadata['sample_filename_pos'][0]):
                sample_filename_pos = metadata['sample_filename_pos'][0]
                massive_id = metadata['massive_id'][0]    
                gnps_dashboard_link = f'https://gnps-lcms.ucsd.edu/?usi=mzspec:{massive_id}:{sample_filename_pos}'
                gnps_tic_pic = f'https://gnps-lcms.ucsd.edu/mspreview?usi=mzspec:{massive_id}:{sample_filename_pos}'
                link_to_massive = f'https://massive.ucsd.edu/ProteoSAFe/dataset.jsp?accession={massive_id}'
                g.add((sample, ns_kg.has_LCMS_pos, rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_pos'][0])))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_pos'][0]), RDF.type, ns_kg.LCMSAnalysisPos))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_pos'][0]), ns_kg.has_gnpslcms_link, rdflib.URIRef(gnps_dashboard_link)))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_pos'][0]), ns_kg.has_massive_doi, rdflib.URIRef(link_to_massive)))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_pos'][0]), ns_kg.has_massive_license, rdflib.URIRef("https://creativecommons.org/publicdomain/zero/1.0/")))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_pos'][0]), FOAF.depiction, rdflib.URIRef(gnps_tic_pic))) 
                
        if set(['sample_filename_neg', 'massive_id']).issubset(metadata.columns):
            if not pd.isna(metadata['sample_filename_neg'][0]):
                sample_filename_neg = metadata['sample_filename_neg'][0]
                massive_id = metadata['massive_id'][0]    
                gnps_dashboard_link = f'https://gnps-lcms.ucsd.edu/?usi=mzspec:{massive_id}:{sample_filename_neg}'
                gnps_tic_pic = f'https://gnps-lcms.ucsd.edu/mspreview?usi=mzspec:{massive_id}:{sample_filename_neg}'
                link_to_massive = f'https://massive.ucsd.edu/ProteoSAFe/dataset.jsp?accession={massive_id}'
                g.add((sample, ns_kg.has_LCMS_neg, rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_neg'][0])))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_neg'][0]), RDF.type, ns_kg.LCMSAnalysisNeg))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_neg'][0]), ns_kg.has_gnpslcms_link, rdflib.URIRef(gnps_dashboard_link)))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_neg'][0]), ns_kg.has_massive_doi, rdflib.URIRef(link_to_massive)))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_neg'][0]), ns_kg.has_massive_license, rdflib.URIRef("https://creativecommons.org/publicdomain/zero/1.0/")))
                g.add((rdflib.term.URIRef(enpkg_uri + metadata['sample_filename_neg'][0]), FOAF.depiction, rdflib.URIRef(gnps_tic_pic))) 
           
        # Add WD taxonomy link to substance
        metadata_taxo_path = os.path.join(path, directory, 'taxo_output', directory + '_taxo_metadata.tsv')
        try:
            metadata_taxo = pd.read_csv(metadata_taxo_path, sep='\t')
            if not pd.isna(metadata_taxo['wd.value'][0]):
                wd_id = rdflib.term.URIRef(WD + metadata_taxo['wd.value'][0][31:])
                g.add((material_id, ns_kg.has_wd_id, wd_id))
                g.add((wd_id, RDF.type, ns_kg.WDTaxon))
            else:
                g.add((material_id, ns_kg.has_unresolved_taxon, rdflib.term.URIRef(enpkg_uri + 'unresolved_taxon')))              
        except FileNotFoundError:
            g.add((material_id, ns_kg.has_unresolved_taxon, rdflib.term.URIRef(enpkg_uri + 'unresolved_taxon')))
              
    elif metadata.sample_type[0] == 'blank':
        g.add((sample, RDF.type, ns_kg.LabBlank))
        g.add((sample, RDFS.label, rdflib.term.Literal(f"Blank {metadata.sample_id[0]}")))
    elif metadata.sample_type[0] == 'qc':
        g.add((sample, RDF.type, ns_kg.LabQc))
        g.add((sample, RDFS.label, rdflib.term.Literal(f"QC {metadata.sample_id[0]}")))

pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, 'metadata_enpkg.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Results are in : {pathout}')