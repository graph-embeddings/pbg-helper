## KNN Graph viewer

TODO: put image

KNN viewer allows queries:
* subj + relation -> obj. Use direction = left
* relation + obj -> subj. Use direction = right

### Installation

Create a conda environment:
```
conda create -n pbg --y python=3.6
conda activate pbg
```
Install `faiss`
```
conda install faiss-cpu -c pytorch
```
And requirements
```pip install -r requirements.txt```

### Requirements
Follow generate_names README. You will need it to create files `entity_name_file` and `relation_name_file`.

### How to launch?

```
ROOT="/var/opt/data/user_data/r.beaumont/pbg"

python api.py --ent_paths='["$ROOT/embeddings_all_0.v30.h5", \
"$ROOT/embeddings_all_1.v30.h5", \
"$ROOT/embeddings_all_2.v30.h5", \
"$ROOT/embeddings_all_3.v30.h5"]' \
--rel_path="$ROOT/model.v30.h5" \
--entity_name_file="viewfs://prod-am6/user/r.beaumont/freebase_names" \
--relation_name_file="viewfs://prod-am6/user/r.beaumont/freebase_relation_names" \
--port=5007
```