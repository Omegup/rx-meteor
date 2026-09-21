import { Mongo } from 'meteor/mongo'
import { Observable, OperatorFunction, Subscription as RxSubscription } from 'rxjs'
import { distinctUntilChanged, map } from 'rxjs/operators'
import isEqual from 'fast-deep-equal'

import { mergeFields as intersectFields } from './merge-fields'
import { publish } from './publish'
import { Model, Param, Watchers } from './types'
import { getValueByKey, omit, pick, subOptionals } from './util'
import { PublishSub } from './watch'

/** Hint forwarded to `cursor.find`. A string names an index; an object is a key pattern. */
export type QueryHint = string | Record<string, unknown> | Record<string, unknown>[]

/**
 * Build a set of named publications over a collection map.
 *
 * `db` is any object whose values are `Mongo.Collection`s. `operate` runs only
 * for publications created with `restricted: true`. It receives the id list
 * from upstream and must return cursors. Unrestricted publications call
 * `collection.find` directly.
 *
 * Returned `simplePublish` / `autoPublish` functions match {@link Publish}.
 * Wire `this.onStop(() => handle.unsubscribe())` in `Meteor.publish`: a
 * Meteor publish handle calls `stop()`, and the value returned here is an
 * RxJS `Subscription`.
 *
 * Queries that do not mention `deletedAt` are restricted to
 * `{ deletedAt: null }`, which also matches documents where the field is absent.
 */
export const makeWatchers = <Pub, MK extends string, M extends Record<MK, { _id?: string }>, DB>(
  db: DB,
  operate: <k extends keyof Pub & MK>(
    name: k & keyof DB & string,
    sub: PublishSub,
    collection: Mongo.Collection<M[k] & Model>,
    query: string | Mongo.Selector<M[k]>,
    path: string | undefined,
    opt: {
      sort?: Mongo.SortSpecifier
      skip?: number
      limit?: number
      fields?: Mongo.FieldSpecifier
      hint?: QueryHint
    },
  ) => OperatorFunction<string[] | null, Mongo.Cursor<M[k] & Model>[]>,
) => {
  const watchers = {} as Watchers<Pub>

  const simplePublish = <k extends keyof Pub & MK>(
    name: k & keyof DB & string,
    sub: PublishSub,
    ids: Observable<string[] | null>,
    [query = {}, { bind = {}, transform: _1 = null, reactive: _2 = undefined, ...opt } = {}, listId]: Param<
      Pub,
      M,
      k
    > = [],
    ready: () => void = () => {},
    path?: string,
    restricted = false,
  ) => {
    try {
      const collection = getValueByKey<DB, Mongo.Collection<M[k] & Model>>(db, name as keyof DB)
      if (query && typeof query === 'object' && 'deletedAt' in query) {
        if (!query.deletedAt) delete (query as { deletedAt?: unknown }).deletedAt
      } else {
        query = restrict(query, { deletedAt: { $eq: null } } as Mongo.Query<Model>)
      }
      return publish<M[k] & Model, Pub>(
        sub,
        !restricted
          ? ids.pipe(
              map(ids => {
                try {
                  return [collection.find(restrictIds(ids, query, path), opt)]
                } catch {
                  return []
                }
              }),
            )
          : ids.pipe(distinctUntilChanged(isEqual), operate<k>(name, sub, collection, query, path, opt)),
        watchers,
        bind,
        ready,
        typeof listId === 'string' ? ['list', listId] : listId,
      )
    } catch (e) {
      console.error('error')
      console.error(e)
      return RxSubscription.EMPTY
    }
  }

  const mergeFields = <k extends keyof Pub & MK>(
    [query = {}, { fields = {}, ...options } = {}, listId]: Param<Pub, M, k> = [],
    restriction: { [_ in keyof M[k]]?: number },
  ): Param<Pub, M, k> => [query, { ...options, fields: intersectFields(fields, restriction as Record<string, number>) }, listId]

  const autoPublish =
    <k extends keyof Pub & MK>(
      name: k & keyof DB & string,
      restricted = false,
      fields?: { [_ in keyof M[k]]?: number },
    ) =>
    (
      sub: PublishSub,
      ids: Observable<string[] | null>,
      params: Param<Pub, M, k>,
      ready: () => void,
      path?: string,
    ) =>
      simplePublish<k>(name, sub, ids, fields ? mergeFields(params, fields) : params, ready, path, restricted)

  return { watchers, simplePublish, mergeFields, autoPublish }
}

/** Combine selectors. Later queries fill keys the earlier ones do not already set, then any remainder is appended to `$and`. */
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

/**
 * AND `added` into `query`. A string query is treated as `{ _id: query }`.
 * A selector with `$query` is updated on that modifier.
 */
export const restrict = <T extends Model>(
  query: string | Mongo.Selector<T>,
  added: Mongo.Query<T>,
): Mongo.Selector<T> => {
  if (!query) return added
  const asQuery = subOptionals<Mongo.Query<T>>()
  if (typeof query === 'string') return $and(asQuery({ _id: query }), added)
  if ('$query' in query) {
    const q = query as Mongo.QueryWithModifiers<T>
    return { ...q, $query: $and(q.$query, added) }
  }
  return $and(query, added)
}

/**
 * Limit `query` to `ids` on `path` (default `_id`).
 * `null` ids leave the query unchanged, which is how a top-level publication selects "everything matching the query".
 * `many` matches an array field via `$elemMatch`.
 */
export function restrictIds<T extends Model>(
  ids: unknown[] | null,
  query: string | Mongo.Selector<T>,
  path: string = '_id',
  many?: boolean,
): string | Mongo.Selector<T> {
  if (!ids) return query
  const asQuery = subOptionals<Mongo.Query<T>>()
  return restrict(query, asQuery({ [path]: many ? { $elemMatch: { $in: ids } } : { $in: ids } }))
}
