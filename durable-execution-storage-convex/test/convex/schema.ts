import { defineSchema, defineTable } from 'convex/server'
import { v } from 'convex/values'

export default defineSchema({
  placeholders: defineTable({
    index: v.number(),
  }).index('by_index', ['index']),
})
