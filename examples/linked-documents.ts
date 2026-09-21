/**
 * When a post is published, also publish the user stored on `post.authorId`.
 * The child publication receives those ids and selects `{ _id: { $in: ids } }`.
 */
import { Meteor } from 'meteor/meteor'
import { Mongo } from 'meteor/mongo'
import { of } from 'rxjs'
import { map } from 'rxjs/operators'

import { makeWatchers, restrictIds, type Param, type PublishSub } from '../src'

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
