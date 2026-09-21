/**
 * Selectors used inside a publication, or on their own before `collection.find`.
 */
import { $and, mergeFields, restrict, restrictIds } from '../src'

type Post = { _id?: string; title: string; authorId: string; deletedAt?: Date | null }

const visible = restrict<Post>({ title: { $exists: true } }, { deletedAt: { $eq: null } })

const forAuthors = restrictIds<Post>(['user-1', 'user-2'], visible, 'authorId')

const titledAndOpen = $and<Post>({ title: 'Hello' }, { deletedAt: null })

const fields = mergeFields({ title: 1, authorId: 1, secret: 1 }, { title: 1, authorId: 1, secret: 0 })

export { visible, forAuthors, titledAndOpen, fields }
