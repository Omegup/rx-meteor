import { Subscription } from 'meteor/meteor'
import { Mongo } from 'meteor/mongo'
import { Observable, OperatorFunction, Subscription as RxSubscription } from 'rxjs'
import { distinctUntilChanged, map, } from 'rxjs/operators'
import isEqual from 'fast-deep-equal'
import { pick, omit, id, getValueByKey, subOptionals } from 'rx-meteor-util'
import type { Hint } from 'mongodb'

import { publish } from './publish'
import { Model, Param, Watchers } from './types'


export const merge = (optFields: Mongo.FieldSpecifier, restriction: Mongo.FieldSpecifier) => {
  const restrictionValues = Object.values(restriction ?? {})
  const fieldsNumberKeys = Object.keys(optFields).filter(k => typeof optFields[k] === 'number')
  let fields: Mongo.FieldSpecifier = pick(optFields, ...fieldsNumberKeys);
  if (restrictionValues.length) {
    const values = Object.values(fields)
    const [picks, omits]: string[][] = [[], []]
    Object.entries(restriction).forEach(([v, k]) => typeof k === 'number' && (v && fields[k] ? picks : omits).push(`${k}`))
    if (!values.length) { // if no field restriction sent, use directly role restriction
      fields = restriction
    } else if (values.some(id)) { // if user requires only some fields, 
      fields = omit(pick(fields, ...picks), ...omits)
    } else if (restrictionValues.some(id)) {
      fields = omit(pick(restriction, ...picks), ...Object.keys(fields))
    } else {
      fields = { ...fields, ...restriction }
    }
    if (!Object.keys(fields).length) fields = { _id: 1 }
    const meta = omit(optFields, ...fieldsNumberKeys)
    return Object.values(fields).some(id) ? { ...fields, ...meta } : { ...meta, ...fields }
  }

}


export const makeWatchers = <Pub, MK extends string, M extends Record<MK, {_id?: string}>, DB>(
  db: DB, operate: <k extends keyof Pub & MK>(
  name: k & KeysMatching<DB, Mongo.Collection<M[k] & Model>> & string,
  sub: Subscription,
  collection: Mongo.Collection<M[k] & Model>,
  query: string | Mongo.Selector<M[k]>,
  path: string | undefined,
  opt: {
    sort?: Mongo.SortSpecifier, skip?: number, limit?: number, fields?: Mongo.FieldSpecifier, hint?: Hint;
  }
) => OperatorFunction<string[], Mongo.Cursor<M[k]>[]>) => {
  const watchers = {} as Watchers<Pub>


  const simplePublish = <k extends keyof Pub & MK>(
    name: k & KeysMatching<DB, Mongo.Collection<M[k] & Model>> & string,
    sub: Subscription,
    ids: Observable<string[] | null>,
    [query = {}, { bind = {}, transform: _1 = null, reactive: _2 = undefined, ...opt } = {}, listId]: Param<Pub, M, k> = [],
    path?: string,
    restricted = false,

  ) => {

    try {

      const collection = getValueByKey<DB, Mongo.Collection<M[k] & Model>>(db, name)

      if (query && typeof query === 'object' && 'deletedAt' in query) {
        if (!query.deletedAt) delete query.deletedAt
      } else {
        query = restrict(query, { deletedAt: { $eq: null } } as Mongo.Query<Model>)
      }
      return publish<M[k], M, Pub>(sub, !restricted
        ? ids.pipe(map(ids => {
          try {
            return [collection.find(restrictIds(ids, query, path), opt)]
          } catch { return [] }
        }))
        : ids.pipe(
          distinctUntilChanged(isEqual),
          operate<k>(name, sub, collection, query, path, opt)
        ), watchers, bind, typeof listId === 'string' ? ['list', listId] : listId
      )
    } catch (e) {
      console.error('error')
      console.error(e)
      return RxSubscription.EMPTY
    }
  }


  const mergeFields = <k extends keyof Pub & MK>(
    [query = {}, { fields = {}, ...options } = {}, listId]: Param<Pub, M, k> = [], restriction: { [_ in keyof M[k]]?: number }
  ): Param<Pub, M, k> => [query, { ...options, fields: merge(fields, restriction as Record<string, number>) }, listId]



  const autoPublish = <k extends keyof Pub & MK>(
    name: k & KeysMatching<DB, Mongo.Collection<M[k] & Model>> & string,
    restricted = false,
    fields?: { [_ in keyof M[k]]?: number },
    hint?: (params: Param<Pub, M, k>, ids: import('rxjs').Observable<string[] | null>, path?: string) => string
  ) => (
    sub: Subscription,
    ids: import('rxjs').Observable<string[] | null>,
    params: Param<Pub, M, k>,
    path?: string
  ) => {
      const p = hint?.(params, ids, path)
      if (p) {
        params = [...params]
        params[1] = { ...(params[1] ?? {}), hint: p }
      }
      return simplePublish<k>(name, sub, ids, fields ? mergeFields(params, fields) : params, path, restricted)
    }
  return { watchers, simplePublish, mergeFields, autoPublish }

}


export const $and = <T extends { _id?: string }>(...queries: Mongo.Query<T>[]): Mongo.Query<T> => {
  queries[0] = queries[0] ?? {}
  for (let i = 1, n = queries.length; i < n; i++) {
    let added: Mongo.Query<T> = queries[i] ?? {}
    if (added.$and) {
      queries[0].$and = [...(queries[0].$and ?? []), ...added.$and]
      delete added.$and
    }
    for (let j = 0; j < i; j++) {
      const query = queries[j]
      queries[j] = { ...query, ...omit(added, ...Object.keys(query)) }
      added = pick(added, ...Object.keys(query)) as Mongo.Query<T>
    }
    queries[i] = added
  }
  queries = queries.filter(q => Object.keys(q ?? {}).length)
  if (queries.length < 2) return queries[0]
  let query = queries[0]
  query.$and = [...(query.$and ?? []), ...queries.slice(1)]
  return query
}

export const restrict = <T extends Model>(query: string | Mongo.Selector<T>, added: Mongo.Query<T>): Mongo.Selector<T> => {
  if (!query) return added
  const asQuery = subOptionals<Mongo.Query<T>>()
  if (typeof query === 'string') return $and(asQuery({ _id: query }), added)
  if ('$query' in query) {
    const q = query as Mongo.QueryWithModifiers<T>
    return { ...q, $query: $and(q.$query, added) }
  }
  else return $and(query, added)
}

export function restrictIds<T extends Model>(ids: string[] | null, query: string | Mongo.Selector<T>, path: string = '_id'): string | Mongo.Selector<T> {
  if (!ids) return query
  const asQuery = subOptionals<Mongo.Query<T>>()
  return restrict(query, asQuery({ [path]: { $in: ids } }))
}

