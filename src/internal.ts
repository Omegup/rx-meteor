/**
 * Helpers used to build publications. The main entry re-exports the stable API.
 * This path can change without a major version.
 */
export {
  setLog,
  logSub,
  startsWith,
  inc,
  dec,
  init,
  _addId,
  addIds,
  _removeId,
  removeIds,
  updateIds,
  getMap,
  changesOn,
  observeChanges,
  merge as mergeDocs,
  rmTimestamp,
} from './watch'
export type { PublishSub, Watched, CB, Store, ObserveChangesCallbacks } from './watch'
