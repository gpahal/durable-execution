import type { StandardSchemaV1 } from '@standard-schema/spec'
import { Effect } from 'effect'

import {
  convertEffectToPromise,
  convertMaybePromiseOrEffectToEffect,
  convertMaybePromiseToEffect,
  convertPromiseToEffect,
  summarizeStandardSchemaIssues,
} from '../src/utils'

describe('utils', () => {
  describe('convertPromiseToEffect', () => {
    it('should convert promise to effect', async () => {
      const effect = convertPromiseToEffect(() => Promise.resolve('test'))
      const result = await Effect.runPromise(effect)
      expect(result).toBe('test')
    })

    it('should convert promise to effect with error', async () => {
      const effect = convertPromiseToEffect(() => Promise.reject(new Error('test')))
      await expect(Effect.runPromise(effect)).rejects.toThrow('test')
    })
  })

  describe('convertMaybePromiseToEffect', () => {
    it('should convert promise to effect', async () => {
      let effect = convertMaybePromiseToEffect(() => Promise.resolve('test'))
      let result = await Effect.runPromise(effect)
      expect(result).toBe('test')

      effect = convertMaybePromiseToEffect(() => 'test')
      result = await Effect.runPromise(effect)
      expect(result).toBe('test')
    })

    it('should convert promise to effect with error', async () => {
      let effect = convertMaybePromiseToEffect(() => Promise.reject(new Error('test')))
      await expect(Effect.runPromise(effect)).rejects.toThrow('test')

      effect = convertMaybePromiseToEffect(() => {
        throw new Error('test')
      })
      await expect(Effect.runPromise(effect)).rejects.toThrow('test')
    })
  })

  describe('convertMaybePromiseOrEffectToEffect', () => {
    it('should convert promise to effect', async () => {
      let effect = convertMaybePromiseOrEffectToEffect(() => Promise.resolve('test'))
      let result = await Effect.runPromise(effect)
      expect(result).toBe('test')

      effect = convertMaybePromiseOrEffectToEffect(() => 'test')
      result = await Effect.runPromise(effect)
      expect(result).toBe('test')

      effect = convertMaybePromiseOrEffectToEffect(() => Effect.succeed('test'))
      result = await Effect.runPromise(effect)
      expect(result).toBe('test')
    })

    it('should convert promise to effect with error', async () => {
      let effect = convertMaybePromiseOrEffectToEffect(() => Promise.reject(new Error('test')))
      await expect(Effect.runPromise(effect)).rejects.toThrow('test')

      effect = convertMaybePromiseOrEffectToEffect(() => {
        throw new Error('test')
      })
      await expect(Effect.runPromise(effect)).rejects.toThrow('test')

      effect = convertMaybePromiseOrEffectToEffect(() => Effect.fail(new Error('test')))
      await expect(Effect.runPromise(effect)).rejects.toThrow('test')
    })
  })

  describe('convertEffectToPromise', () => {
    it('should convert effect to promise', async () => {
      const promise = convertEffectToPromise(Effect.succeed('test'))
      const result = await promise
      expect(result).toBe('test')
    })

    it('should convert effect to promise with error', async () => {
      const promise = convertEffectToPromise(Effect.fail(new Error('test')))
      await expect(promise).rejects.toThrow('test')
    })
  })

  describe('summarizeStandardSchemaIssues', () => {
    it('should summarize single issue without path', () => {
      const issues: Array<StandardSchemaV1.Issue> = [
        {
          message: 'Invalid value',
        },
      ]

      const summary = summarizeStandardSchemaIssues(issues)
      expect(summary).toBe('× Invalid value')
    })

    it('should summarize single issue with path', () => {
      const issues: Array<StandardSchemaV1.Issue> = [
        {
          message: 'Required field',
          path: [{ key: 'user' }, { key: 'email' }],
        },
      ]

      const summary = summarizeStandardSchemaIssues(issues)
      expect(summary).toBe('× Required field\n  → at user.email')
    })

    it('should summarize multiple issues with mixed paths', () => {
      const issues: Array<StandardSchemaV1.Issue> = [
        {
          message: 'Invalid email format',
          path: [{ key: 'email' }],
        },
        {
          message: 'Password too short',
          path: [{ key: 'password' }],
        },
        {
          message: 'Age must be positive',
          path: [{ key: 'profile' }, { key: 'age' }],
        },
      ]

      const summary = summarizeStandardSchemaIssues(issues)
      expect(summary).toBe(
        '× Invalid email format\n  → at email\n' +
          '× Password too short\n  → at password\n' +
          '× Age must be positive\n  → at profile.age',
      )
    })

    it('should handle issues without paths correctly', () => {
      const issues: Array<StandardSchemaV1.Issue> = [
        {
          message: 'General validation error',
        },
        {
          message: 'Another error',
          path: [{ key: 'field' }],
        },
      ]

      const summary = summarizeStandardSchemaIssues(issues)
      expect(summary).toBe('× General validation error\n' + '× Another error\n  → at field')
    })

    it('should handle empty issues array', () => {
      const issues: Array<StandardSchemaV1.Issue> = []
      const summary = summarizeStandardSchemaIssues(issues)
      expect(summary).toBe('')
    })

    it('should handle complex nested paths with arrays', () => {
      const issues: Array<StandardSchemaV1.Issue> = [
        {
          message: 'Invalid item',
          path: [{ key: 'orders' }, { key: 0 }, { key: 'items' }, { key: 2 }, { key: 'price' }],
        },
      ]

      const summary = summarizeStandardSchemaIssues(issues)
      expect(summary).toBe('× Invalid item\n  → at orders.0.items.2.price')
    })

    it('should handle multiple issues with no paths', () => {
      const issues: Array<StandardSchemaV1.Issue> = [
        { message: 'First error' },
        { message: 'Second error' },
        { message: 'Third error' },
      ]

      const summary = summarizeStandardSchemaIssues(issues)
      expect(summary).toBe('× First error\n' + '× Second error\n' + '× Third error')
    })

    it('should handle single character paths', () => {
      const issues: Array<StandardSchemaV1.Issue> = [
        {
          message: 'Invalid field',
          path: [{ key: 'a' }, { key: 'b' }, { key: 'c' }],
        },
      ]

      const summary = summarizeStandardSchemaIssues(issues)
      expect(summary).toBe('× Invalid field\n  → at a.b.c')
    })
  })
})
