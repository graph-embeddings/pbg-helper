import fire
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_restful import Resource, Api

from graph_knn import load_entity_knn, load_multi_knn


class Knn(Resource):
    def __init__(self, **kwargs):
        self.knn = kwargs['knn']

    def post(self):
        json_data = request.get_json(force=True)
        entity_uri = json_data["entity"]
        relation_uri = json_data["relation"]
        k = int(json_data["k"])
        direction = json_data["direction"]
        uris, dists, names = self.knn.find_entity_knn(entity_uri, relation_uri, k, direction)
        response = [{'uri': uri, 'dist': float(dist), 'name': name} for [uri, dist, name] in
                    zip(uris, dists, names)]
        return jsonify(response)


class EntitySearch(Resource):
    def __init__(self, **kwargs):
        self.ent_dict_name = kwargs['ent_dict_name']

    def post(self):
        json_data = request.get_json(force=True)
        query = json_data["query"]
        limit = int(json_data["limit"])
        offset = int(json_data["offset"])
        result = [{'value': k, 'label': v} for k, v in self.ent_dict_name.items() if
                  query.lower() in v.lower()]
        filtered = result[offset:(offset + limit)]
        response = {'result': filtered, 'size': len(result)}
        return jsonify(response)


class RelationSearch(Resource):
    def __init__(self, **kwargs):
        self.rel_dict_uri = kwargs['rel_dict_uri']

    def post(self):
        json_data = request.get_json(force=True)
        query = json_data["query"]
        limit = int(json_data["limit"])
        offset = int(json_data["offset"])
        result = [{'value': k, 'label': k} for k, v in self.rel_dict_uri.items() if
                  query.lower() in k.lower()]
        filtered = result[offset:(offset + limit)]
        response = {'result': filtered, 'size': len(result)}
        return jsonify(response)


class IndexedEntitySearch(Resource):
    def __init__(self, **kwargs):
        self.entity_index = kwargs['entity_index']

    def post(self):
        json_data = request.get_json(force=True)
        query = json_data["query"]
        limit = int(json_data["limit"])
        offset = int(json_data["offset"])
        result = [{'value': entity.uri, 'label': f'{entity.name} {entity.count} {entity.uri}'}
                  for entity in self.entity_index.find_entity(query)]
        filtered = result[offset:(offset + limit)]
        response = {'result': filtered, 'size': len(result)}
        return jsonify(response)


class IndexedRelationSearch(Resource):
    def __init__(self, **kwargs):
        self.entity_index = kwargs['entity_index']

    def post(self):
        json_data = request.get_json(force=True)
        query = json_data["query"]
        limit = int(json_data["limit"])
        offset = int(json_data["offset"])
        result = [{'value': entity.uri, 'label': f'{entity.name} {entity.count} {entity.uri}'}
                  for entity in self.entity_index.find_entity(query)]
        filtered = result[offset:(offset + limit)]
        response = {'result': filtered, 'size': len(result)}
        return jsonify(response)


def launch_api(ent_path, rel_path, dict_path, name_dict_path):
    app = Flask(__name__)
    api = Api(app)
    knn = load_entity_knn(ent_path, rel_path, dict_path, name_dict_path)
    api.add_resource(Knn, "/knn", resource_class_kwargs={'knn': knn})
    api.add_resource(EntitySearch, "/knn-entity-search",
                     resource_class_kwargs={'ent_dict_name': knn.ent_dict_name})
    api.add_resource(RelationSearch, "/knn-relation-search",
                     resource_class_kwargs={'rel_dict_uri': knn.rel_dict_uri})
    CORS(app)
    app.run(host="0.0.0.0", port="5006")


def launch_api_multi(ent_paths, rel_path, entity_name_file, relation_name_file, port):
    app = Flask(__name__)
    api = Api(app)
    knn = load_multi_knn(ent_paths, rel_path, entity_name_file, relation_name_file)
    api.add_resource(Knn, "/knn", resource_class_kwargs={'knn': knn})
    api.add_resource(IndexedEntitySearch, "/knn-entity-search",
                     resource_class_kwargs={'entity_index': knn.entity_index})
    api.add_resource(RelationSearch, "/knn-relation-search",
                     resource_class_kwargs={'rel_dict_uri': knn.relation_index.uri_to_entity})
    CORS(app)
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    fire.Fire(launch_api_multi)
