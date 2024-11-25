import { TupleNode } from 'kysely';
import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  languageCounts = {};
  
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    for (const post of ops.posts.creates) {
      if (post.record.langs && post.record.langs.indexOf('en') < 0 && post.record.langs.indexOf('en-US') < 0) {
        let lastLang = ''
        for (const lang of post.record.langs) {
          if (lang in this.languageCounts) {
            this.languageCounts[lang]++
          } else {
            this.languageCounts[lang] = 1
          }
          lastLang = lang
        }
        this.languageCounts = Object.entries(this.languageCounts).sort(([l1, c1]: [string, number],[l2, c2]: [string, number]) => c2 - c1).reduce((o, [k,v]) => { o[k] = v; return o}, {})
        console.log(lastLang.toUpperCase())
        console.log(post.record.text)
        console.log("Language counts:", JSON.stringify(this.languageCounts))
        console.log("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  
      }
    }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only alf-related posts
        return create.record.text.toLowerCase().includes('alf')
      })
      .map((create) => {
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
