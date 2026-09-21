import type { PublishSub } from './watch'

/** Child publications keyed by collection name. The tuple is `[params, path?]`. */
export type Field<Pub> = { [k in keyof Pub]?: [Pub[k], string?] }

/** `(sub, ids, params, ready, path?) => Subscription`. `ids === null` means the query is not limited to an id list. */
export type Publish<Pub, k extends keyof Pub> = (
  sub: PublishSub,
  ids: import('rxjs').Observable<string[] | null>,
  params: Pub[k],
  ready: () => void,
  path?: string,
) => import('rxjs').Subscription

export type Watchers<Pub> = {
  [k in keyof Pub]: Publish<Pub, k>
}

/**
 * Arguments for one publication: `[query, options, listId]`.
 * `options.bind` maps a field of this document to child publications.
 * `options.fields` is a Mongo field specifier. `listId`, when set, also publishes
 * membership documents on the `list` collection.
 */
export type Param<Pub, M, k extends keyof M> = [
  (import('meteor/mongo').Mongo.Selector<M[k]> | string)?,
  (import('meteor/mongo').Mongo.Options<M[k]> & {
    bind?: { [_: string]: Field<Pub> } & { [_ in keyof M[k]]?: Field<Pub> }
  })?,
  (string | null)?,
]

export interface Model {
  _id?: string
  deletedAt?: Date | null
}
