import os
from rdflib import Graph

# These lines allows to make sure that we are placed at the repo directory level 
from pathlib import Path

p = Path(__file__).parents[2]
os.chdir(p)

g = Graph()
for file in os.listdir("./data/out"):
    if (file.endswith(".ttl")) & (file != 'test_full.ttl'):
        g.parse(os.path.join("./data/out", file))

pathout = os.path.normpath("./data/out/")
g.serialize(destination="./data/out/test_full.ttl", format="ttl", encoding="utf-8")
