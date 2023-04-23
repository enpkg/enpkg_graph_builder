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

## 1. Steps that are sample's specific

### 1.1 Format samples' metadata

```console
python .\src\01_rdf_sample_metadata.py -p path/to/your/data/directory/
```

### 1.2 Format feature's
:warning: Part of this script needs to be adataped to you data
```console
python .\src\02_rdf_features.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

### 1.3 Format Sirius/CSI:FingerID annotations

```console
python .\src\03_rdf_csi_annotations.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```


### 1.4 Format Canopus annotations

```console
python .\src\03_rdf_canopus.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```


### 1.5 Format ISDB annotations

```console
python .\src\05_rdf_isdb_annotations.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

### 1.6 Format samples' FBMN

```console
python .\src\06_rdf_individual_mn.py -p path/to/your/data/directory/ -ion {pos} or {neg}
```

## 2. Steps that are dataset specific/optional

### 2.1. Format GNPS meta-MN

```console
python .\src\07_rdf_meta_mn.py -p path/to/your/data/directory/  -ion {pos} or {neg} -id {gnps_job_id} -metadata {The metadata file corresonding to the aggregated .mgf uploaded on GNPS}
```

### 2.2. Format structures' metadata

```console
python .\src\08_rdf_structures_metadata.py -p path/to/your/data/directory/ -db {The path to the structures metadata SQL DB}
```

### 2.3. Format ChEMBL structures' metadata

```console
python .\src\08_rdf_structures_metadata.py -p path/to/your/data/directory/ -chemdb {The path to the structures metadata SQL DB} -biodb {The path to the samples ChEMBL metadata FOLDER (will integrate all ChEMBL files)}
```
## 3. Compress to gzip
Compress to gzip the produced .ttl files whose size exceed 200Mb to allow for GraphDB import.
```console
python .\src\10_gzip_rdf.py -p path/to/your/data/directory/
```

