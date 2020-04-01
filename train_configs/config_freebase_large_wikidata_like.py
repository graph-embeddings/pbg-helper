#!/usr/bin/env python3

'''
torchbiggraph_train \
  reco-graph/config_freebase_large_wikidata_like.py  \
  -p edge_paths=data/freebase/large/edges
'''

'''
torchbiggraph_eval \
  reco-graph/config_freebase_large_wikidata_like.py \
  -p edge_paths=data/freebase_valid/large/edges \
  -p relations.0.all_negs=false \
  -p num_uniform_negs=0
'''

def get_torchbiggraph_config():

    config = dict(
        # I/O data
        entity_path="data/freebase/large",
        edge_paths=[
            "data/freebase/large/edges"
            # "data/freebase/large/graph_v0_valid-partitioned",
            # "data/freebase/large/graph_v0_test-partitioned",
        ],
        checkpoint_path="model/freebase/large",

        # Graph structure
        entities={
            'all': {'num_partitions': 4},
        },
        relations=[{
            'name': 'all_edges',
            'lhs': 'all',
            'rhs': 'all',
            'operator': 'complex_diagonal',
        }],
        dynamic_relations=True,

        # Scoring model
        dimension=100,
        global_emb=False,
        comparator='dot',

        # Training
        num_epochs=3,
        num_edge_chunks=10,
        batch_size=10000,
        num_batch_negs=500,
        num_uniform_negs=500,
        loss_fn='softmax',
        relation_lr=0.01,
        lr=0.1,

        # Evaluation during training
        eval_fraction=0.001,
        eval_num_batch_negs=10000,
        eval_num_uniform_negs=0,

        # Misc
        background_io=False,
        verbose=1,
    )

    return config