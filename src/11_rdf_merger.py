from rdflib import Graph

# Create an empty graph
merged_graph = Graph()

# Iterate over the files and add their contents to the merged graph
for file_path in ["rdf/canopus_pos.ttl", "rdf/features_pos.ttl", "rdf/features_spe2vec_pos.ttl", "rdf/indivual_mn_pos.ttl", "rdf/isdb_pos.ttl", "rdf/metadata_enpkg.ttl", "rdf/sirius_pos.ttl"]:
    with open(file_path, "r") as f:
        file_content = f.read()
        merged_graph.parse(data=file_content, format="ttl")

# Create a new graph with unique triples
unique_graph = Graph()
unique_graph += list(set(merged_graph))

# Write the unique graph to a file
with open("merged_data.ttl", "w") as f:
    f.write(unique_graph.serialize(format="ttl").decode())