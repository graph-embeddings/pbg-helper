import JsonBigint from 'json-bigint'

export default class KnnService {
  constructor (backend) {
    this.backend = backend
  }

  knnSearch (entity, relation, k, direction) {
    return fetch(this.backend + '/knn', {
    method: 'POST',
    body: JsonBigint
      .stringify({
          entity,
          relation,
          k,
          direction
        })
      })
      .then(response => response.text())
      .then(response => JsonBigint.parse(response))
  }

  entitySearch (query, limit, offset) {
    return fetch(this.backend + '/knn-entity-search', {
    method: 'POST',
    body: JsonBigint.stringify({
      query,
      limit,
      offset
    })
    })
    .then(response => response.text())
    .then(response => JsonBigint.parse(response))
  }

  relationSearch (query, limit, offset) {
      return fetch(this.backend + '/knn-relation-search', {
    method: 'POST',
    body: JsonBigint.stringify({
      query,
      limit,
      offset
    })
    })
    .then(response => response.text())
    .then(response => JsonBigint.parse(response))
  }
}
