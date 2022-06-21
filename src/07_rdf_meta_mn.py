import itertools
import os
import pandas as pd
import rdflib
from rdflib import Graph
import networkx as nx
import itertools
import subprocess
import shlex
import zipfile

# These lines allows to make sure that we are placed at the repo directory level 
from pathlib import Path

p = Path(__file__).parents[2]
os.chdir(p)


job_id = '07b0304b8119469e9be135b4752c482c'                      
data_in_path = './data/in/vgf_unaligned_data_test/'
data_in_path_mn = './data/in/'


def gnps_job_fetcher(gnps_job_id, input_folder):
    """Fetch a GNPS job and saves its to a given folder
    Args:
            gnps_job_id (str): a GNPS Job id
            input_folder (str): the folder where the job should be kept
    Returns:
        nothing
    """
    path_to_folder = os.path.expanduser(
        os.path.join(input_folder, gnps_job_id))
    path_to_file = os.path.expanduser(
        os.path.join(input_folder, gnps_job_id + '.zip'))

    print('''
    Fetching the GNPS job: '''
    + gnps_job_id
    )
    # or &view=download_clustered_spectra or download_cytoscape_data (check when to use which and optionalize)
    job_url_zip = "https://gnps.ucsd.edu/ProteoSAFe/DownloadResult?task=" + \
        gnps_job_id+"&view=download_cytoscape_data"

    cmd = 'curl -d "" '+job_url_zip+' -o '+path_to_file + ' --create-dirs'
    subprocess.call(shlex.split(cmd))

    with zipfile.ZipFile(path_to_file, 'r') as zip_ref:
        zip_ref.extractall(path_to_folder)

    # We finally remove the zip file
    os.remove(path_to_file)

    print('''
    Job successfully downloaded: results are in: '''
    + path_to_folder
    )

gnps_job_fetcher(job_id,data_in_path_mn)

# path to meta MN
for file in os.listdir(os.path.join('./data/in/' + job_id + '/gnps_molecular_network_graphml')):
    mn_graphml_path = os.path.join('./data/in/' + job_id + '/gnps_molecular_network_graphml/' + file)
# path feature ID key
feature_key_path = os.path.join(data_in_path + 'vgf_pos_aggregated_metadata_sandbox.csv')
# path cluster id key
for file in os.listdir(os.path.join('./data/in/' + job_id + '/clusterinfo')):
    cluster_id_path = os.path.join('./data/in/' + job_id + '/clusterinfo/' + file)


g = Graph()
nm = g.namespace_manager

# Create jlw namespace
jlw_uri = "https://www.sinergiawolfender.org/jlw/"
ns_jlw = rdflib.Namespace(jlw_uri)
prefix = "mkg"
nm.bind(prefix, ns_jlw)

nx_graph = nx.read_graphml(mn_graphml_path)

feature_key = pd.read_csv(feature_key_path)
dic_feature_id_to_original_feature_id = pd.Series(feature_key.original_feature_id.values,index=feature_key.feature_id).to_dict()
len(dic_feature_id_to_original_feature_id)
cluster_id = pd.read_csv(cluster_id_path, sep= '\t')
dic_feature_id_to_cluster_id = pd.Series(cluster_id['#ClusterIdx'].values,index=cluster_id['#Scan']).to_dict()
len(dic_feature_id_to_cluster_id)


# create triples for features in the same cluster index
for ci in list(cluster_id['#ClusterIdx'].unique()):    
    ms_cluster_linked = [k for k,v in dic_feature_id_to_cluster_id.items() if v == ci]
    if len(ms_cluster_linked) > 1:
        for pair in itertools.combinations(ms_cluster_linked,2):
            s = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[pair[0]])
            t = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[pair[1]])
            g.add((s, ns_jlw.is_mscluster_similar_to, t))

# create triples for features link in MN         
for node in nx_graph.edges(data=True):
    if node[0] != node[1]:
        s_ci = int(node[0])
        t_ci = int(node[1])
        s_features = [k for k,v in dic_feature_id_to_cluster_id.items() if v == s_ci]
        t_features = [k for k,v in dic_feature_id_to_cluster_id.items() if v == t_ci]
        for pair in itertools.product(s_features, t_features):
            s = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[pair[0]])
            t = rdflib.term.URIRef(jlw_uri + dic_feature_id_to_original_feature_id[pair[1]])
            g.add((s, ns_jlw.is_gnps_meta_mn_similar_to, t))

pathout = os.path.normpath("./data/out/")
os.makedirs(pathout, exist_ok=True)
g.serialize(destination="./data/out/meta_mn.ttl", format="ttl", encoding="utf-8")
