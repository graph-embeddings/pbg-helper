## PBG Helper

This is a helper for [PyTorch-BigGraph](https://github.com/facebookresearch/PyTorch-BigGraph/tree/master/torchbiggraph). Two main contributions:
* `dataset_importer` to generate partition dataset for large data ( >  200M edges)
* `knn-graph-viewer` to visualize results

TODO: put image

### Usage

1. import a dataset, see [dataset importer README](dataset_importer/README.md)
2. train, see example [freebase training config](train_configs/config_freebase_large_wikidata_like.py). Optionally run the evaluation.
3. generate names for entities and relations for visualization, see [generate names README](generate_names/README.md)
4. launch viewer, see [knn graph viewer README](knn-graph-viewer/back/README.md)

