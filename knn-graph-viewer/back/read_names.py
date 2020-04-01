from collections import defaultdict
from typing import NamedTuple

import pandas
import pyarrow.parquet as pq


class Entity(NamedTuple):
    name: str
    uri: str
    count: int
    index: int
    partition: int


# "hdfs://root/user/r.beaumont/freebase_names"
class EntityNameIndex:
    def __init__(self, word_to_uri, uri_to_entity, partition_index_to_entity):
        self.word_to_uri = word_to_uri
        self.uri_to_entity = uri_to_entity
        self.partition_index_to_entity = partition_index_to_entity

    @staticmethod
    def generate_from_entity_file(entity_path):
        names = pq.read_table(entity_path)
        p = names.to_pandas()
        c = 0
        word_to_uri = defaultdict(list)
        uri_to_entity = {}
        partition_index_to_entity = {}
        for name, uri, count, index, partition in zip(p['name'], p['ent'], p['count'], p['index'], p['partition']):
            entity = Entity(name, uri, int(count), int(index), int(partition))
            uri_to_entity[uri] = entity
            partition_index_to_entity[(entity.partition, entity.index)] = entity
            parts = name.lower().split(" ")
            for part in parts:
                word_to_uri[part].append(uri)
            c = c + 1
            if c % 100000 == 0:
                print(f"{c} done")

        return EntityNameIndex(word_to_uri, uri_to_entity, partition_index_to_entity)

    def find_entity(self, query):
        parts = query.lower().split(" ")
        uris = list(set.intersection(*[set(self.word_to_uri[part]) for part in parts]))
        entities = [self.uri_to_entity[uri] for uri in uris]
        entities.sort(key=lambda e: -e.count)
        return entities

