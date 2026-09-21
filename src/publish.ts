import { Mongo } from 'meteor/mongo'
import { Observable, Subscription as RxSubscription, Subject } from 'rxjs'
import { map, sample, take } from 'rxjs/operators'

import { addIds, CB, dec, inc, init, PublishSub, removeIds, updateIds, watch } from './watch'
import { Field, Watchers } from './types'

const tuple = <T extends any[]>(...args: T) => args

const get = (obj: any[], path: string[]): any[] =>
  path.length ? get(obj.flatMap(o => o?.[path[0]]), path.slice(1)) : obj

const getField = (id: string, e: unknown[], field: string): string[] =>
  (field === '_id' ? [id] : get(e, field.split('.'))).filter(x => x)

/**
 * Watch `cursors$` and, for each field in `binds`, publish the linked collections.
 *
 * `binds` maps a field on the parent document to `{ [collection]: [childParams, path?] }`.
 * When a parent is added or changed, the ids stored on that field are sent to the
 * child watcher. `ready` runs after the first parent batch and every child watcher
 * has called its own ready. Pass `() => sub.ready()`.
 *
 * `list` publishes an extra `listName` document per id (`listId + id`) so a client
 * can tell which parents belong to this subscription.
 */
export const publish = <T extends { _id?: string }, Pub>(
  sub: PublishSub,
  cursors$: Observable<Mongo.Cursor<T>[]>,
  watchers: Watchers<Pub>,
  binds: Record<string, Field<Pub>>,
  ready: () => void,
  list?: [string, string] | null,
  prefix = '',
) => {
  const subs = new RxSubscription()
  subs.add(() => inc(sub))
  type W = ReturnType<typeof init<{ _id: string }, T>>
  const linkedEntries = Object.entries(binds).map(([k]) => tuple(k, init<{ _id: string }, T>()))
  const linkedModelsIds: Record<string, W> = Object.fromEntries(linkedEntries)
  const notifier = new Subject<void>()
  const promises: Promise<void>[] = []
  Object.entries(linkedModelsIds).forEach(([name, list]) => {
    const specs = Object.entries(binds[name] ?? {}) as [string, [unknown, string?] | undefined][]
    specs.forEach(([collection, spec]) => {
      promises.push(
        new Promise<void>(resolve => {
          const watcher = watchers[collection as keyof Pub]
          if (!watcher || !spec) {
            if (!watcher) console.error(`Watcher for collection ${collection} not found`)
            resolve()
            return
          }
          const [params, path] = spec
          subs.add(
            watcher(
              sub,
              list.obs.pipe(
                map(ids => ids.map(([id]) => id)),
                sample(notifier),
              ),
              params as Pub[keyof Pub],
              resolve,
              path,
            ),
          )
        }),
      )
    })
  })
  notifier
    .pipe(take(1))
    .toPromise()
    .then(() => Promise.all(promises))
    .then(ready)
  const cb: CB<T, () => void> = {
    notif: () => {
      Object.entries(linkedModelsIds).forEach(([, list]) => {
        if (!list.emitted) {
          list.obs.next([...list.set.entries()])
          list.emitted = true
        }
      })
      notifier.next()
    },
    added: ({ id, fields: e }) =>
      Object.entries(linkedModelsIds).forEach(([field, list]) => {
        const ref = getField(id, [e], field)
        if (ref.length) addIds(list, ref, id)
      }),
    changed: ({ id, updated: e }) => {
      const cbs: (() => void)[] = []
      Object.entries(linkedModelsIds).forEach(([field, list]) => {
        const ref = getField(id, [e], field)
        if (ref.length) cbs.push(updateIds(list, ref, id))
      })
      return { ctx: () => cbs.forEach(cb => cb()) }
    },
    afterChanged: ({ ctx }) => ctx?.(),
    afterRemoved: ({ id }) =>
      Object.values(linkedModelsIds).forEach(list => removeIds(list, id, { optional: true })),
  }
  if (list) {
    const [listName, listId] = list
    cb.afterAdded = ({ id }) => {
      sub.added(listName, listId + id, { list: listId })
    }
    cb.removed = ({ id }) => {
      sub.removed(listName, listId + id)
    }
  }
  subs.add(watch<T, () => void>(sub, cursors$, cb, list !== null, prefix))
  subs.add(() => dec(sub))
  return subs
}
