/* globals customElements */

import { LitElement, html, unsafeCSS, css } from 'lit-element'
import '@vaadin/vaadin-combo-box'
import KnnService from './knn-service'
import 'wct-datatables-net'
import bootstrapCss from 'bootstrap/dist/css/bootstrap.min.css'

class KnnGraphViewer extends LitElement {
  constructor () {
    super()
    this.backend = ''
    this.direction = 'left'
    this.entity = '/m/0clpml'
    this.relation = '/film/film/genre'
    this.entityLabel = 'James bond'
    this.k = 5
    this.error = ''
    this.loadStatus = 'not_loaded'
  }

  static get properties () {
    return {
      backend: {
        type: String
      },
      direction: {
        type: String
      },
      entity: {
        type: String
      },
      entityLabel: {
        type: String
      },
      relation: {
        type: String
      },
      k: {
        type: Number
      },
      count: {
        type: String
      },
      error: {
        type: String
      },
      loadStatus: {
        type: String
      },
      tableOptions: {
        type: Object
      },
      service: { type: Object }
    }
  }

  search () {
    if (this.entity === '' || this.relation === '') {
      this.loadStatus = 'error'
      this.error = 'please choose an entity and relation'
      return
    }
    this.loadStatus = 'loading'
    this.service.knnSearch(this.entity, this.relation, this.k, this.direction)
      .then(async r => {
        const dataSet = r.map(({ dist, name }) => {
            return [
              name,
              dist.toString()
            ]
        })
        console.log(dataSet)
        this.tableOptions = {
          ordering: false,
          destroy: true,
          data: dataSet,
          paging: dataSet.length > 10,
          info: dataSet.length > 10,
          columns: [
            { title: 'Entity' },
            { title: 'Distance' }]
        }
        this.loadStatus = 'loaded'
      }).catch(err => {
        this.loadStatus = 'error'
        this.error = err.message
      })
  }

  updated (changedProperties) {
    if (changedProperties.has('backend')) {
      this.service = new KnnService(this.backend)
    }
    if (changedProperties.has('relation') || changedProperties.has('entity') ||
      changedProperties.has('k') || changedProperties.has('backend') ||
      changedProperties.has('direction')) {
      this.search()
    }
  }

  static get styles () {
    return [
      unsafeCSS(bootstrapCss),
      css`
      * {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";

       }
       .container {
           max-width: 92%;
           margin-left: 1%;
           margin-right: 5%;
       }
      `
    ]
  }

  render () {
    return html`
      <div class="container">
      <h2>
         Knn graph            
      </h2>
      <p>Query a knn service and get instant results.</p>

    <div style="margin-bottom: 30px">
        <div class="form-group row">
            <label for="backend" class="col-sm-3 col-form-label">Backend</label>
            <div class="col-sm-8">
                <input class="form-control" type="text" id="backend" value="${this.backend}"
                @change=${e => { this.backend = e.target.value }}" 
                placeholder="your knn viewer backend. http://localhost:5007"/>
            </div>
        </div>

        <div class="form-group row">
            <label for="entity" class="col-sm-3 col-form-label">Entity</label>
            <div class="col-sm-8">
                 <vaadin-combo-box
                 @selected-item-changed=${e => {
    this.entity = e.detail.value.value
    this.entityLabel = e.detail.value.label
  }}
                 .selectedItem=${{ label: this.entityLabel, value: this.entity }}
                 .dataProvider = ${(params, callback) => {
    const index = params.page * params.pageSize
    this.service.entitySearch(params.filter, params.pageSize, index)
      .then(response => callback(response.result, response.size))
  }} style="width:100%;"></vaadin-combo-box>
            </div>
        </div>


        <div class="form-group row">
            <label for="relation" class="col-sm-3 col-form-label">Relation</label>
            <div class="col-sm-8">
                <vaadin-combo-box @value-changed=${e => { this.relation = e.detail.value }}
                .selectedItem=${this.relation}
                 .dataProvider = ${(params, callback) => {
    const index = params.page * params.pageSize
    this.service.relationSearch(params.filter, params.pageSize, index)
      .then(response => callback(response.result, response.size))
  }} style="width:100%;"></vaadin-combo-box>
            </div>
        </div>

        <div class="form-group row">
            <label for="direction" class="col-sm-3 col-form-label">Direction</label>
            <div class="col-sm-8">
                <select class="form-control" id="direction" @change=${e => { this.direction = e.target.value }}>
                    ${[
    ['left', 'left'],
    ['right', 'right']
  ]
    .map(([id, name]) => html`<option value="${id}" ?selected=${this.direction === id}>${name}</option>`)}
                  </select>
            </div>
        </div>
        <div class="form-group row">
            <label for="k" class="col-sm-3 col-form-label">k</label>
            <div class="col-sm-8">
                <input class="form-control" type="text" id="k" value="${this.k}"
                @change=${e => { this.k = e.target.value }}" />
            </div>
        </div>

        <div class="row">
            <div class="col-sm-2">
                <button type="submit" @click=${() => this.search()} class="btn btn-primary">Search</button>
            </div>

            <progress-status class="col-md-5" .status=${this.loadStatus} .error=${this.error}></progress-status>
        </div>

        <data-table .options=${this.tableOptions}
        ></data-table>
    </div>
</div>`
  }
}

customElements.define('knn-graph-viewer', KnnGraphViewer)
