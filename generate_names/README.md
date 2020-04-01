## Generate names for KNN Graph viewer

To search entities and relations in the viewer we need to map indexes with names. 
For this the viewer reads a parquet file with columns `name`, `ent`, `count`, `index`, `partition`.

We provide two examples to generate `entity_names` file and `relations_names` file.

### Example 1: generate names only from dataset

In `example_generate_names.scala`, the generated names correspond to names provided when running `dataset_importer`.
The caveat is that it might not be the most human readable names.


### Example 2: generate names for freebase

In `get_freebase_names.scala`, we generate names from raw freebase data.