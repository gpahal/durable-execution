import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import * as td from 'typedoc'

const scriptDir = path.dirname(fileURLToPath(import.meta.url))
const rootDir = path.join(scriptDir, '..')
const readmePath = path.join(rootDir, 'README.md')
const docsDir = path.join(rootDir, 'docs')

async function buildDocs() {
  console.log('Building docs')

  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'durable-execution-docs-'))
  try {
    const tmpReadmePath = path.join(tmpDir, 'README.md')
    let readmeContent = await fs.readFile(readmePath, 'utf8')
    readmeContent = readmeContent.trim().split('\n').slice(1).join('\n')
    await fs.writeFile(tmpReadmePath, readmeContent)

    const app = await td.Application.bootstrapWithPlugins({
      name: 'durable-execution',
      plugin: ['typedoc-plugin-mermaid'],
      entryPoints: ['src/index.ts'],
      entryPointStrategy: 'resolve',
      readme: tmpReadmePath,
      hostedBaseUrl: 'https://gpahal.github.io/durable-execution',
      navigationLinks: {
        GitHub: 'https://github.com/gpahal/durable-execution',
      },
      includeVersion: true,
      excludePrivate: true,
      excludeProtected: false,
      excludeInternal: true,
      defaultCategory: 'Other',
      categoryOrder: ['Executor', 'Task', 'Storage', 'Errors', 'Serializer', 'Logger', 'Other'],
      navigation: {
        includeCategories: true,
      },
      sort: ['kind', 'source-order'],
      highlightLanguages: ['ts', 'js', 'bash', 'json', 'markdown'],
      validation: {
        notExported: true,
        invalidLink: true,
        rewrittenLink: true,
        notDocumented: false,
        unusedMergeModuleWith: true,
      },
      treatWarningsAsErrors: true,
      treatValidationWarningsAsErrors: true,
    })

    const project = await app.convert()
    if (!project) {
      throw new Error('Failed to convert to project')
    }

    await app.generateDocs(project, docsDir)
  } finally {
    await fs.rm(tmpDir, { recursive: true })
  }
}

await buildDocs()
