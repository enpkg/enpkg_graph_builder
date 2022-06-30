import itertools
import os
from re import sub
import pandas as pd
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF, RDFS, XSD
import networkx as nx
import itertools
import subprocess
import shlex
import zipfile
import argparse
import textwrap

# These lines allows to make sure that we are placed at the repo directory level 
from pathlib import Path

p = Path(__file__).parents[1]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        TO DO
        '''))

parser.add_argument('-p', '--sample_dir_path', required=True,
                    help='The path to the directory where samples folders to process are located')
parser.add_argument('-ion', '--ionization_mode', required=True,
                    help='The ionization mode to process')
parser.add_argument('-id', '--gnps_job_id', required=True,
                    help='The GNPS job id')
parser.add_argument('-m', '--metadata', required=True,
                    help='The the metadata file corresonding to the aggregated .mgf uploaded on GNPS')

args = parser.parse_args()
sample_dir_path = os.path.normpath(args.sample_dir_path)
job_id = args.gnps_job_id
ionization_mode = args.ionization_mode
metadata = args.metadata

sample_dir_path = os.path.normpath('C:/Users/gaudrya.FARMA/Desktop/ordered')
job_id = '822f2d6ea4a34d18b059689597b06cf4'
ionization_mode = 'neg'
metadata = '220615_agg_spectra_neg_metadata.csv'

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

# Create jlw namespace
jlw_uri = "https://www.sinergiawolfender.org/jlw/"
ns_jlw = rdflib.Namespace(jlw_uri)
prefix = "enpkg"
nm.bind(prefix, ns_jlw)

g.add((ns_jlw.GNPSAnnotation, RDFS.subClassOf, ns_jlw.Annotation))

# Load data
nx_graph = nx.read_graphml(mn_graphml_path)

feature_key = pd.read_csv(feature_key_path)
dic_feature_id_to_original_feature_id = pd.Series(feature_key.original_feature_id.values,index=feature_key.feature_id).to_dict()

cluster_id = pd.read_csv(cluster_id_path, sep= '\t')
dic_feature_id_to_cluster_id = pd.Series(cluster_id['#ClusterIdx'].values,index=cluster_id['#Scan']).to_dict()

cluster_summary = pd.read_csv(cluster_summary_path, sep= '\t')

annotation_summary = pd.read_csv(annotation_summary_path, sep= '\t')
annotation_summary.dropna(subset=['InChIKey-Planar'], inplace=True)
dic_library_id_to_ik2D = pd.Series(annotation_summary['InChIKey-Planar'].values,index=annotation_summary['SpectrumID']).to_dict()

# Add consensus spectrum nodes
# mask = cluster_id['#ClusterIdx'].duplicated(keep=False)
# cluster_id_dup = cluster_id[mask]
for _, row in cluster_id.iterrows():
    feature = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[row['#Scan']])
    consensus = rdflib.term.URIRef(jlw_uri + 'GNPS_consensus_spectrum_' + ionization_mode + '_ClusterID_' + str(row['#ClusterIdx']))
    g.add((feature, ns_jlw.has_consensus_spectrum, consensus))
    cluster_link = cluster_summary[cluster_summary['cluster index']==row['#ClusterIdx']]['GNPSLinkout_Cluster'].values[0]
    g.add((consensus, ns_jlw.gnps_spectrum_link, rdflib.term.Literal(cluster_link)))
    network_link = cluster_summary[cluster_summary['cluster index']==row['#ClusterIdx']]['GNPSLinkout_Network'].values[0]
    g.add((consensus, ns_jlw.gnps_component_link, rdflib.term.Literal(network_link)))
    usi = 'mzspec:MassIVE:TASK-' + job_id + '-spectra/specs_ms.mgf:scan:' + str(row['#ClusterIdx'])
    g.add((consensus, ns_jlw.has_usi, rdflib.term.Literal(usi)))
    link_spectrum = 'https://metabolomics-usi.ucsd.edu/dashinterface/?usi1=' + usi
    g.add((consensus, ns_jlw.gnps_dashboard_view, rdflib.term.Literal(link_spectrum)))
    g.add((consensus, ns_jlw.has_component_index, rdflib.term.Literal(cluster_summary[cluster_summary['cluster index']==row['#ClusterIdx']]['componentindex'].values[0], datatype=XSD.integer)))
    g.add((consensus, RDF.type, ns_jlw.MS2Spectrum))
    
# # create triples for features in the same cluster index
# for ci in list(cluster_id['#ClusterIdx'].unique()):    
#     ms_cluster_linked = [k for k,v in dic_feature_id_to_cluster_id.items() if v == ci]
#     if len(ms_cluster_linked) > 1:
#         for pair in itertools.combinations(ms_cluster_linked,2):
#             s = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[pair[0]])
#             t = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[pair[1]])
#             g.add((s, ns_jlw.is_mscluster_similar_to, t))

# create triples for features link in MN         
for node in nx_graph.edges(data=True):
    if node[0] != node[1]:
        s_ci = int(node[0])
        t_ci = int(node[1])
        s_consensus = rdflib.term.URIRef(jlw_uri + 'GNPS_consensus_spectrum_' + ionization_mode + '_ClusterID_' + str(s_ci))
        t_consensus = rdflib.term.URIRef(jlw_uri + 'GNPS_consensus_spectrum_' + ionization_mode + '_ClusterID_' + str(t_ci))
        g.add((s_consensus, ns_jlw.is_cosine_similar_to, t_consensus))
        g.add((t_consensus, ns_jlw.is_cosine_similar_to, s_consensus))
        # for pair in itertools.product(s_features, t_features):
        #     s = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[pair[0]])
        #     t = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[pair[1]])
        #     g.add((s, ns_jlw.is_cosine_similar_to, t))

# Add annotation to consensus nodes
annotated_spectra = cluster_summary.dropna(subset='SpectrumID')
for _, row in annotated_spectra.iterrows():
    if row['SpectrumID'] in dic_library_id_to_ik2D.keys():
        cluster_id = rdflib.term.URIRef(jlw_uri + 'GNPS_consensus_spectrum_' + ionization_mode + '_ClusterID_' + str(row['cluster index']))
        gnps_annotation_id = rdflib.term.URIRef(jlw_uri + str(row['SpectrumID']))
        usi = 'mzspec:GNPS:GNPS-LIBRARY:accession:' + str(row['SpectrumID'])
        link_spectrum = 'https://metabolomics-usi.ucsd.edu/dashinterface/?usi1=' + usi
        InChIkey2D = rdflib.term.URIRef(jlw_uri + dic_library_id_to_ik2D[row['SpectrumID']])
        g.add((cluster_id, ns_jlw.has_gnps_annotation, gnps_annotation_id))
        g.add((gnps_annotation_id, ns_jlw.has_InChIkey2D, InChIkey2D))
        g.add((gnps_annotation_id, ns_jlw.has_usi, rdflib.term.Literal(usi)))
        g.add((gnps_annotation_id, ns_jlw.gnps_dashboard_view, rdflib.term.Literal(link_spectrum)))
        g.add((gnps_annotation_id, RDF.type, ns_jlw.GNPSAnnotation))
    
    
    
pathout = os.path.join(sample_dir_path, "004_rdf/")
os.makedirs(pathout, exist_ok=True)
pathout = os.path.normpath(os.path.join(pathout, f'meta_mn_{ionization_mode}.ttl'))
g.serialize(destination=pathout, format="ttl", encoding="utf-8")
print(f'Result are in : {pathout}') 
