# rx-meteor

RxJS helpers for Meteor publications. Watch Mongo cursors, republish when the cursor set changes, and follow ids from one document to another. Source: [github.com/Omegup/rx-meteor](https://github.com/Omegup/rx-meteor).

The package does not know your models. You pass collections in, and you get cursors out.

Meteor 3 is the target. `Cursor.observeChanges` returns a promise there and delivers the initial `added` events before that promise resolves. `watch` awaits each handle and runs cursor batches one at a time, so a later emission cannot stop an observe that has not finished starting.

## Install

```sh
meteor npm install rx-meteor rxjs
```

`rxjs` is a peer dependency (`^6.6.7` or `^7`). Use 6 if that is what the Meteor app already has.

This package only runs inside a Meteor app. It imports `meteor/meteor` and `meteor/mongo`, which the Meteor bundler provides.

## Publications

`makeWatchers` turns a map of collections into publish functions.

```ts
import { Meteor } from 'meteor/meteor'
import { Mongo } from 'meteor/mongo'
import { of } from 'rxjs'
import { map } from 'rxjs/operators'
import { makeWatchers, restrictIds, type Param, type PublishSub } from 'rx-meteor'

type Post = { _id?: string; title: string; authorId: string; deletedAt?: Date | null }
type User = { _id?: string; name: string; deletedAt?: Date | null }
type Models = { posts: Post; users: User }
type Pubs = {
  posts: Param<Pubs, Models, 'posts'>
  users: Param<Pubs, Models, 'users'>
}

const db = {
  posts: new Mongo.Collection<Post>('posts'),
  users: new Mongo.Collection<User>('users'),
}

const { watchers, autoPublish } = makeWatchers<Pubs, keyof Models, Models, typeof db>(
  db,
  (_name, _sub, collection, query, path, opt) =>
    map(ids => {
      try {
        return [collection.find(restrictIds(ids, query, path), opt)]
      } catch {
        return []
      }
    }),
)

watchers.posts = autoPublish('posts')

Meteor.publish('posts', function () {
  const handle = watchers.posts(this as unknown as PublishSub, of(null), [{}, {}], () => this.ready())
  this.onStop(() => handle.unsubscribe())
})
```

`of(null)` means the id list is not restricted. The query is used as given.

Call `this.ready()` from the `ready` argument. `publish` waits for the first cursor batch, then for every linked child, then calls it. An empty result still becomes ready.

Return value is an RxJS `Subscription`. Meteor stops a publication by calling `stop()` on the handle, and `Subscription` does not have `stop`. Use `onStop`.

Meteor's public `Subscription` type does not list `_subscriptionId` or `_subscriptionHandle`. The publish context still has them at runtime, so the examples cast `this` through `unknown`.

The third argument is a [`Param`](#param) tuple: `[query, options, listId]`.

A query that does not mention `deletedAt` also requires `{ deletedAt: null }`. That matches documents where the field is missing. If the collection does not use soft delete, leave this as is, or pass `deletedAt` yourself when you need something else.

`autoPublish(name)` finds directly. `autoPublish(name, true)` sends the id list through the operator you gave `makeWatchers`. Use that when the cursor depends on the current user, for example a permission check. The same operator is what linked documents use to turn ids into a cursor.

Full file: [`examples/publication.ts`](examples/publication.ts).

## Linked documents

`options.bind` maps a field on the parent document to another publication. When a post is added or its `authorId` changes, the `users` publication is given those ids and selects `{ _id: { $in: ids } }`.

```ts
watchers.posts = autoPublish('posts')
watchers.users = autoPublish('users', true)

Meteor.publish('posts', function () {
  const handle = watchers.posts(
    this as unknown as PublishSub,
    of(null),
    [{}, { bind: { authorId: { users: [[{}]] } } }],
    () => this.ready(),
  )
  this.onStop(() => handle.unsubscribe())
})
```

`{ users: [childParam, path?] }` uses `childParam` as the child [`Param`](#param). An unrestricted child is `{ users: [[{}]] }`: the inner tuple is `[query, options, listId]`. `path` defaults to `_id`. Set it when the child id lives on another field.

The parent publication does not become ready until each child has become ready.

Full file: [`examples/linked-documents.ts`](examples/linked-documents.ts).

## Watch a cursor yourself

Use `watch` when you already have an `Observable<Cursor[]>`. Each emission is the full set of cursors to observe. Cursors that remain in the array keep their handle. Cursors that disappear are stopped, and their documents are removed.

```ts
import { watch, type PublishSub } from 'rx-meteor'

watch<Post>(this as PublishSub, cursors$, {
  added: ({ id, fields }) => {
    if (fields.title === 'hidden') return { v: 'ignore' }
  },
  changed: ({ fields }) => ({ v: fields }),
})
```

`publish` defaults to `true` and writes `added`, `changed`, and `removed` on the subscription. Pass `false` to run the callbacks without DDP traffic.

A callback may return:

| Return | Effect |
| --- | --- |
| nothing | Send the Mongo fields. |
| `{ v: fields }` | Send `fields` instead. |
| `{ v: 'ignore' }` | Do not send this document. |
| `{ ctx }` | Pass `ctx` to the matching `afterAdded`, `afterChanged`, or `afterRemoved`. |

`setLog(1)` prints cursor starts, adds, changes, and removes. `setLog(0)` turns that off.

## Queries

These are the selectors `simplePublish` uses. They are exported so a custom operator can use the same rules.

```ts
import { $and, mergeFields, restrict, restrictIds } from 'rx-meteor'

restrict<Post>({ title: { $exists: true } }, { deletedAt: { $eq: null } })

restrictIds<Post>(['user-1', 'user-2'], { title: { $exists: true } }, 'authorId')

$and<Post>({ title: 'Hello' }, { deletedAt: null })

mergeFields({ title: 1, authorId: 1, secret: 1 }, { title: 1, authorId: 1, secret: 0 })
// { title: 1, authorId: 1 }
```

`restrictIds(null, query)` returns `query`. That is the `of(null)` case.

`mergeFields` intersects two field specifiers. A field stays included only when the restriction includes it and the caller asked for it. An empty caller specifier means the restriction is used as-is. The handle returned by `makeWatchers` also has `mergeFields(params, restriction)`, which applies that intersection to `params[1].fields`.

`restrictIds(..., many: true)` matches an array field with `$elemMatch: { $in: ids }`.

Full file: [`examples/queries.ts`](examples/queries.ts).

## API

| Export | Role |
| --- | --- |
| `makeWatchers(db, operate)` | Returns `{ watchers, simplePublish, autoPublish, mergeFields }`. |
| `publish(sub, cursors$, watchers, binds, ready, list?, prefix?)` | Watch cursors and start the publications named in `binds`. |
| `watch(sub, cursors$, callbacks, publish?, prefix?)` | Observe cursors and optionally write DDP. |
| `restrict`, `restrictIds`, `$and` | Build selectors. |
| `mergeFields` | Intersect field specifiers. |
| `setLog` | Toggle tracing. |
| `PublishSub`, `Param`, `Field`, `Watchers`, `Publish`, `CB`, `Model` | Public types. |

`PublishSub` is the slice of a Meteor publish context these functions call: `added`, `changed`, `removed`, `connection.id`, `_subscriptionId`, `_subscriptionHandle`. `this` inside `Meteor.publish` has those fields at runtime. The public Meteor types omit the two underscored ones, so a cast through `unknown` is required.

### Param

`[query, options, listId]`

- `query` is a selector or an `_id` string. Omitted means `{}`.
- `options` is forwarded to `find` (`sort`, `skip`, `limit`, `fields`, `hint`). `bind` is the link map described above. `transform` and `reactive` are ignored.
- `listId`, when it is a string, also publishes one document per parent id on the collection `list`, with `_id` equal to `listId + id` and `{ list: listId }`. Clients use that to know which documents belong to this subscription. `null` skips it.

`simplePublish` is the same function with the collection name and the restricted flag filled in. `autoPublish(name, restricted?, fields?)` pre-fills the name and, when `fields` is set, intersects them into every call.

## `rx-meteor/internal`

Id-graph bookkeeping (`init`, `addIds`, `removeIds`, `updateIds`), `observeChanges`, and `rmTimestamp` live on `rx-meteor/internal`. That path is for code that already built on the 0.0.3 exports. It is not part of the supported API and can change without a major version.

## Build

`dist/` is compiled CommonJS plus declarations. `pnpm install` and `npm install` run the build through `prepare`. The Meteor app should depend on the package and restart after it changes.

## From 0.0.3

0.1.0 is the first version meant to be installed from npm.

- The package entry is `dist/index.js`, not TypeScript source.
- `inc`, `dec`, `observeChanges`, and the id-graph helpers moved to `rx-meteor/internal`.
- `merge` was two different functions. Field specifiers are `mergeFields`. Document merging stays on the internal path as `mergeDocs`.
- `mergeFields` keeps restriction field names. 0.0.3 pushed `"0"` and `"1"` instead of those names.
- The unused `rx-meteor-util` dependency is gone.

## License

No license is set yet. Add one before publishing to the public registry.
