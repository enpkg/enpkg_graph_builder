import os
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF, RDFS, XSD
import networkx as nx
import argparse
import textwrap
from pathlib import Path
from tqdm import tqdm

p = Path(__file__).parents[1]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a RDF graph (.ttl format) from sa GNPS meta-MN (classical MN on aggregated unaligned spectra) 
         --------------------------------
            Arguments:
            - Path to the directory where samples folders are located
            - Ionization mode to process
        '''))

parser.add_argument('-p', '--sample_dir_path', required=True,
                    help='The path to the directory where samples folders to process are located')
parser.add_argument('-ion', '--ionization_mode', required=True,
                    help='The ionization mode to process')
parser.add_argument('-id', '--gnps_job_id', required=True,
                    help='The GNPS job id')
parser.add_argument('-m', '--metadata', required=True,
                    help='The metadata file corresonding to the aggregated .mgf uploaded on GNPS')

args = parser.parse_args()
sample_dir_path = os.path.normpath(args.sample_dir_path)
job_id = args.gnps_job_id
ionization_mode = args.ionization_mode
metadata = args.metadata


# path to meta MN
for file in os.listdir(os.path.join(sample_dir_path, '002_gnps',  job_id, 'gnps_molecular_network_graphml')):
    mn_graphml_path = os.path.join(sample_dir_path, '002_gnps',  job_id, 'gnps_molecular_network_graphml/' + file)

# path feature ID key
feature_key_path = os.path.join(sample_dir_path, '001_aggregated_spectra',  metadata)

# path cluster id key
for file in os.listdir(os.path.join(sample_dir_path, '002_gnps',  job_id, 'clusterinfo')):
    cluster_id_path = os.path.join(sample_dir_path, '002_gnps',  job_id, 'clusterinfo', file)

# path clustersummary
for file in os.listdir(os.path.join(sample_dir_path, '002_gnps',  job_id, 'clusterinfosummarygroup_attributes_withIDs_withcomponentID')):
    cluster_summary_path = os.path.join(sample_dir_path, '002_gnps',  job_id, 'clusterinfosummarygroup_attributes_withIDs_withcomponentID', file)

# path annotation summary
for file in os.listdir(os.path.join(sample_dir_path, '002_gnps',  job_id, 'result_specnets_DB')):
    annotation_summary_path = os.path.join(sample_dir_path, '002_gnps',  job_id, 'result_specnets_DB', file)
    
g = Graph()
nm = g.namespace_manager

# Create enpkg namespace
kg_uri = "https://enpkg.commons-lab.org/kg/"
ns_kg = rdflib.Namespace(kg_uri)
prefix = "enpkg"
nm.bind(prefix, ns_kg)

# Load data
nx_graph = nx.read_graphml(mn_graphml_path)

feature_key = pd.read_csv(feature_key_path)
dic_feature_id_to_original_feature_id = pd.Series(feature_key.original_feature_id.values, index=feature_key.feature_id).to_dict()

cluster_id = pd.read_csv(cluster_id_path, sep= '\t')
dic_feature_id_to_cluster_id = pd.Series(cluster_id['#ClusterIdx'].values,index=cluster_id['#Scan']).to_dict()

cluster_summary = pd.read_csv(cluster_summary_path, sep= '\t')

annotation_summary = pd.read_csv(annotation_summary_path, sep= '\t')
annotation_summary.dropna(subset=['InChIKey-Planar'], inplace=True)
dic_library_id_to_ik2D = pd.Series(annotation_summary['InChIKey-Planar'].values,index=annotation_summary['SpectrumID']).to_dict()

# Add consensus spectrum nodes
# mask = cluster_id['#ClusterIdx'].duplicated(keep=False)
# cluster_id_dup = cluster_id[mask]
for _, row in tqdm(cluster_id.iterrows(), total=len(cluster_id)):
    feature = rdflib.term.URIRef(kg_uri + dic_feature_id_to_original_feature_id[row['#Scan']])
    usi = 'mzspec:MassIVE:TASK-' + job_id + '-spectra/specs_ms.mgf:scan:' + str(row['#ClusterIdx'])
    consensus = rdflib.term.URIRef(kg_uri + 'GNPS_consensus_spectrum_' + usi)
    link_spectrum = 'https://metabolomics-usi.ucsd.edu/dashinterface/?usi1=' + usi
    
    g.add((feature, ns_kg.has_consensus_spectrum, consensus))
    cluster_link = cluster_summary[cluster_summary['cluster index']==row['#ClusterIdx']]['GNPSLinkout_Cluster'].values[0]
    g.add((consensus, ns_kg.gnps_spectrum_link, rdflib.term.Literal(cluster_link)))
    network_link = cluster_summary[cluster_summary['cluster index']==row['#ClusterIdx']]['GNPSLinkout_Network'].values[0]
    g.add((consensus, ns_kg.gnps_component_link, rdflib.term.Literal(network_link)))
    g.add((consensus, ns_kg.has_usi, rdflib.term.Literal(usi)))
    g.add((consensus, ns_kg.gnps_dashboard_view, rdflib.term.Literal(link_spectrum)))
    
    component_index = cluster_summary[cluster_summary['cluster index']==row['#ClusterIdx']]['componentindex'].values[0]
    ci_node = rdflib.term.URIRef(kg_uri + 'metamn_' + job_id + '_componentindex_' + str(component_index))
       
    g.add((consensus, ns_kg.has_metamn_ci, ci_node))
    g.add((consensus, RDF.type, ns_kg.GNPSConsensusSpectrum))

# create triples for features link in MN         
for node in nx_graph.edges(data=True):
    if node[0] != node[1]:
        s_ci = int(node[0])
        t_ci = int(node[1])
        s_usi = 'mzspec:MassIVE:TASK-' + job_id + '-spectra/specs_ms.mgf:scan:' + str(s_ci)
        s_consensus = rdflib.term.URIRef(kg_uri + 'GNPS_consensus_spectrum_' + s_usi)
        t_usi = 'mzspec:MassIVE:TASK-' + job_id + '-spectra/specs_ms.mgf:scan:' + str(t_ci)
        t_consensus = rdflib.term.URIRef(kg_uri + 'GNPS_consensus_spectrum_' + t_usi)
        
        cosine = node[2]['cosine_score']
        mass_diff = abs(float(node[2]['mass_difference']))
        
        link_node = rdflib.term.URIRef(kg_uri + 'consensus_pair_' + s_usi + '_' + t_usi)
        g.add((link_node, RDF.type, ns_kg.CSpair))
        g.add((link_node, ns_kg.has_member, s_consensus))
        g.add((link_node, ns_kg.has_member, t_consensus))
        g.add((link_node, ns_kg.has_cosine, rdflib.term.Literal(cosine, datatype=XSD.float)))
        g.add((link_node, ns_kg.has_mass_difference, rdflib.term.Literal(mass_diff, datatype=XSD.float)))

# Add annotation to consensus nodes
annotated_spectra = cluster_summary.dropna(subset='LibraryID')
for _, row in tqdm(annotated_spectra.iterrows(), total = len(annotated_spectra)):
    if row['SpectrumID'] in dic_library_id_to_ik2D.keys():
        usi = 'mzspec:MassIVE:TASK-' + job_id + '-spectra/specs_ms.mgf:scan:' + str(row['cluster index'])
        cluster_id = rdflib.term.URIRef(kg_uri + 'GNPS_consensus_spectrum_' + usi)

        gnps_annotation_id = rdflib.term.URIRef(kg_uri + str(row['SpectrumID']))
        usi = 'mzspec:GNPS:GNPS-LIBRARY:accession:' + str(row['SpectrumID'])
        link_spectrum = 'https://metabolomics-usi.ucsd.edu/dashinterface/?usi1=' + usi
        InChIkey2D = rdflib.term.URIRef(kg_uri + dic_library_id_to_ik2D[row['SpectrumID']])
        g.add((cluster_id, ns_kg.has_gnps_annotation, gnps_annotation_id))
        g.add((gnps_annotation_id, ns_kg.has_InChIkey2D, InChIkey2D))
        g.add((gnps_annotation_id, ns_kg.has_usi, rdflib.term.Literal(usi)))
        g.add((gnps_annotation_id, ns_kg.gnps_dashboard_view, rdflib.term.Literal(link_spectrum)))
        g.add((gnps_annotation_id, RDF.type, ns_kg.GNPSAnnotation))
    
pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, f'meta_mn_{ionization_mode}.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Result are in : {pathout}') 
