import json

import h5py
import faiss
from torchbiggraph.model import ComplexDiagonalDynamicOperator, TranslationDynamicOperator
import torch.nn as nn
import torch
import fire
from read_names import EntityNameIndex


class ComplexOperators:
    def __init__(self, real, imag):
        self.real = real
        self.imag = imag

    def get_pbg_operator(self):
        op = ComplexDiagonalDynamicOperator(self.real.shape[1] + self.imag.shape[1], 1)
        op.real = nn.Parameter(torch.tensor(self.real))
        op.imag = nn.Parameter(torch.tensor(self.imag))
        return op


class TranslationOperators:
    def __init__(self, op):
        self.op = op

    def get_pbg_operator(self):
        op = TranslationDynamicOperator(self.op.shape[1], 1)
        op.translations = nn.Parameter(torch.tensor(self.op))
        return op


class Relations:
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs


# "/var/opt/data/user_data/r.beaumont/pbg/model/fb15k/embeddings_all_0.v50.h5"
def load_entities(path):
    f_ent = h5py.File(path, 'r')
    return f_ent['embeddings']


# "/var/opt/data/user_data/r.beaumont/pbg/model/fb15k/model.v50.h5"
def load_relations(path):
    f_rel = h5py.File(path, 'r')
    lhs = f_rel['model']['relations']['0']['operator']['lhs']
    rhs = f_rel['model']['relations']['0']['operator']['rhs']
    return Relations(ComplexOperators(lhs['real'], lhs['imag']), ComplexOperators(rhs['real'], rhs['imag']))


# eg freebase operators
def load_translations(path):
    f_rel = h5py.File(path, 'r')
    lhs = f_rel['model']['relations']['0']['operator']['lhs']['translations']
    rhs = f_rel['model']['relations']['0']['operator']['rhs']['translations']
    return Relations(TranslationOperators(lhs), TranslationOperators(rhs))


def build_entity_index(entities):
    dim = entities.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(entities[()])
    return index


def find_entity_knn(index, lhs, entity, rel_id, k: int):
    """
    entity: np.array (400,1)
    return_type: tuple of arrays (k nearest entities id, distances)
    """
    projected_mat = lhs.forward(torch.tensor([entity]), torch.tensor([rel_id]).expand(1))
    return index.search(projected_mat.detach().numpy(), k)


def load_entities_uri(path: str):
    """
    two dicts: id -> uri and uri -> id
    """
    d_all = json.load(open(path))
    id2uri = dict(enumerate(d_all["entities"]["all"]))
    uri2id = {v: k for k, v in id2uri.items()}
    return id2uri, uri2id


def load_relations_uri(path):
    """
    two dicts: id -> uri and uri -> id
    """
    d_all = json.load(open(path))
    id2uri = dict(enumerate(d_all["relations"]))
    uri2id = {v: k for k, v in id2uri.items()}
    return id2uri, uri2id


def load_entity_uri_to_name(path):
    """
    :param path:
    :return: json with uri, label
    """
    return json.load(open(path))


class EntityKnn:
    def __init__(self, rel_embs, ent_embs, ent_dict_name,
                 ent_dict_id, ent_dict_uri, rel_dict_uri):
        self.lhs = rel_embs.lhs.get_pbg_operator()
        self.rhs = rel_embs.rhs.get_pbg_operator()
        self.index = build_entity_index(ent_embs)
        self.ent_embs = ent_embs
        self.ent_dict_name = ent_dict_name
        self.ent_dict_id = ent_dict_id
        self.ent_dict_uri = ent_dict_uri
        self.rel_dict_uri = rel_dict_uri

    def find_entity_knn(self, ent_uri, rel_uri, k, direction):
        """
        :param direction:
        :param rel_uri:
        :param ent_uri:
        :param k:
        :return: tuple of arrays: k nearest entities uri, name distances
        """
        rel_id = self.rel_dict_uri[rel_uri]
        entity_id = self.ent_dict_uri[ent_uri]
        entity = self.ent_embs[entity_id]
        operator = self.lhs if direction == 'left' else self.rhs
        dists, ids = find_entity_knn(self.index, operator, entity, rel_id, k)
        uris = [self.ent_dict_id[ent_id] for ent_id in ids[0,]]
        names = [self.ent_dict_name[uri] if uri in self.ent_dict_name else "" for uri in uris]
        return uris, dists.tolist()[0], names


# used for partitioned entities
class MultiEntityKnn:
    def __init__(self, entity_index, relation_index, rel_embs, ent_embs):
        self.entity_index = entity_index
        self.relation_index = relation_index
        self.lhs = rel_embs.lhs.get_pbg_operator()
        self.rhs = rel_embs.rhs.get_pbg_operator()
        self.indices = [build_entity_index(ent_emb) for ent_emb in ent_embs]

    def find_entity_knn(self, ent_uri, rel_uri, k, direction):
        if rel_uri not in self.relation_index.uri_to_entity:
            return [], [], []
        relation = self.relation_index.uri_to_entity[rel_uri]
        if ent_uri not in self.entity_index.uri_to_entity:
            return [], [], []
        entity = self.entity_index.uri_to_entity[ent_uri]

        reconstruction_index = self.indices[entity.partition]
        entity_emb = reconstruction_index.reconstruct(entity.index)
        operator = self.lhs if direction == 'left' else self.rhs
        dist_ids = []
        for partition, index in enumerate(self.indices):
            dists, ids = find_entity_knn(index, operator, entity_emb, relation.index, k)
            for dist, id in zip(dists[0,], ids[0,]):
                if (partition, id) in self.entity_index.partition_index_to_entity:
                    dist_ids.append((dist, id, partition))
        dist_ids.sort(key=lambda e: -e[0])
        k_dist_ids = dist_ids[0:k]
        dists = [dist for dist, _, _ in k_dist_ids]
        entities = [self.entity_index.partition_index_to_entity[(partition, id)] for _, id, partition in k_dist_ids]
        uris = [entity.uri for entity in entities]
        names = [entity.name for entity in entities]
        return uris, dists, names


def load_entity_knn(ent_path, rel_path, dict_path, name_dict_path):
    ent_embs = load_entities(ent_path)
    rel_embs = load_relations(rel_path)
    ent_dict_name = load_entity_uri_to_name(name_dict_path)
    ent_dict_id, ent_dict_uri = load_entities_uri(dict_path)
    rel_dict_id, rel_dict_uri = load_relations_uri(dict_path)
    knn = EntityKnn(rel_embs=rel_embs, ent_embs=ent_embs, ent_dict_name=ent_dict_name,
                    ent_dict_id=ent_dict_id, ent_dict_uri=ent_dict_uri, rel_dict_uri=rel_dict_uri)
    return knn


def load_multi_knn(ent_paths, rel_path, entity_name_file, relation_name_file):
    entity_index = EntityNameIndex.generate_from_entity_file(entity_name_file)
    relation_index = EntityNameIndex.generate_from_entity_file(relation_name_file)
    rel_embs = load_relations(rel_path)
    ent_embs = [load_entities(ent_path) for ent_path in ent_paths]
    knn = MultiEntityKnn(entity_index, relation_index, rel_embs, ent_embs)
    return knn


def full_example(ent_path, rel_path, dict_path, name_dict_path, ent_uri, rel_uri, k):
    knn = load_entity_knn(ent_path, rel_path, dict_path, name_dict_path)
    uris, dists, names = knn.find_entity_knn(ent_uri, rel_uri, k, 'left')
    print(f"entity {knn.ent_dict_name[ent_uri]}")
    print(f"relation {rel_uri}")
    print(f"uris {uris}")
    print(f"dists {dists}")
    print(f"names {names}")


if __name__ == "__main__":
    fire.Fire(full_example)
