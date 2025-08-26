import { defineApp } from 'convex/server'
import taskExecutionsStorage from 'durable-execution-storage-convex/convex.config'

const app = defineApp()
app.use(taskExecutionsStorage)

export default app
