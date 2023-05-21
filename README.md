# enpkg_graph_builder
Build the Experimental Natural Products Knowledge Graph

⚙️ Workflow part of [enpkg_workflow](https://github.com/enpkg/enpkg_workflow).

The aim of this repository is to format as RDF the data produced previously.

## 0. Clone repository and install environment

1. Clone this repository.
2. Create environment: 
```console 
conda env create -f environment.yml
```
3. Activate environment:  
```console 
conda activate graph_builder
```
## 1. Format data as RDF

For each sample, a new folder containing all the produced .ttl files will be created. The workflow is splitted into different scripts so that you can adapt it easily.

## Desired architecture

```
data/
└─── sample_a/
      └─── rdf/
            metadata_enpkg.ttl                        # Sample's metadata
            features_pos.tll                          # Features data in PI mode
            features_neg.ttl                          # Features data in NI mode
            features_spec2vec_pos.tll                 # Features spec2vec data in PI mode
            features_spec2vec_neg.ttl                 # Features spec2vec data in NI mode
            sirius_pos.tll                            # SIRIUS data in PI mode
            sirius_neg.ttl                            # SIRIUS data in NI mode
            canopus_pos.tll                           # CANOPUS data in PI mode
            canopus_neg.ttl                           # CANOPUS data in NI mode
            isdb_pos.tll                              # ISDB data in PI mode
            isdb_neg.ttl                              # ISDB data in NI mode
            individual_mn_pos.tll                     # MN data in PI mode
            individual_mn_neg.ttl                     # MN data in NI mode
            structures_metadata.ttl                   # Structures' metadata
            
            {sample_a}_merged_graph.ttl               # Data from all above subgraphs
```


### 1.1 Format sample's metadata
Generate ```metadata_enpkg.ttl``` 
```console
python .\src\individual_processing\01_a_rdf_enpkg_metadata_indi.py -p path/to/your/data/directory/
```

### 1.2 Format features' data
Generate ```features_{ion}.ttl``` 
```console
python .\src\individual_processing\02_a_rdf_features_indi.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

### 1.3 Format features' spec2vec data
Generate ```features_spec2vec_{ion}.ttl``` 
```console
python .\src\individual_processing\02_b_rdf_features_spec2vec_indi.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

### 1.4 Format SIRIUS annotations
Generate ```sirius_{ion}.ttl``` 
```console
python .\src\individual_processing\03_rdf_csi_annotations_indi.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

### 1.5 Format CANOPUS annotations
Generate ```canopus_{ion}.ttl``` 
```console
python .\src\individual_processing\04_rdf_canopus_indi.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

### 1.6 Format ISDB annotations
Generate ```isdb_{ion}.ttl``` 
```console
python .\src\individual_processing\05_rdf_isdb_annotations_indi.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

### 1.7 Format MN data
Generate ```individual_mn_{ion}.ttl``` 
```console
python .\src\individual_processing\06_rdf_individual_mn_indi.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

### 1.8 Format structures' metadata
Generate ```structures_metadata.ttl```
```console
python .\src\individual_processing\07_rdf_structures_metadata_indi.py -p path/to/your/data/directory/ -db {The path to the structures metadata SQL DB}
```

### 1.9 Merge previously generated .tll files
Generate ```{sample_a}_merged_graph.ttl```
```console
python .\src\individual_processing\08_rdf_merger.py -p path/to/your/data/directory/
```

### 1.10 Export all merged .ttl files into the same folder

```console
python .\src\individual_processing\09_rdf_exporter.py -s path/to/your/data/directory/ -t path/to/your/target/directory/ - c compress to gz (optional)
```

## 2. Steps that are dataset specific/optional

### 2.1. Format ChEMBL structures' metadata

```console
python .\src\rdf_biodereplication.py -chemdb {The path to the structures metadata SQL DB} -biodb {The path to the samples ChEMBL metadata FOLDER (will integrate all ChEMBL files)} -o path/to/your/target/directory/result.ttl
```
