from pathlib import Path
import os
import shutil
import argparse
import textwrap
from tqdm import tqdm

# These lines allows to make sure that we are placed at the repo directory level 
p = Path(__file__).parents[2]
os.chdir(p)

""" Argument parser """
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
        This script generate a unique RDF graph by sample (.ttl format) from multiples sample specific .rdf files.
         --------------------------------
            Arguments:
            - Path to the directory where samples folders are located
        '''))

parser.add_argument('-s', '--source_path', required=True,
                    help='The path to the directory where samples folders to process are located')
parser.add_argument('-t', '--target_path', required=True,
                    help='The path to the directory into wich the .ttl files are copied')

args = parser.parse_args()
source_path = os.path.normpath(args.source_path)
target_path = os.path.normpath(args.target_path)

os.makedirs(target_path, exist_ok=True)

samples_dir = [directory for directory in os.listdir(source_path)]
df_list = []
for directory in tqdm(samples_dir):
    src = os.path.join(source_path, directory, "rdf", f"{directory}_merged_graph.ttl")
    if os.path.isfile(src):
        dst = os.path.join(target_path, f"{directory}_merged_graph.ttl")
        shutil.copyfile(src, dst)
