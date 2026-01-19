import { ErrorContext } from './types'
import { isNonEmptyArray, isNotNullish } from './validators/type-guards'

type SqlBuilderErrorCode =
  | 'FIELD_NOT_FOUND'
  | 'INVALID_OPERATOR'
  | 'INVALID_VALUE'
  | 'RELATION_ERROR'
  | 'PARAM_ERROR'
  | 'SQL_ERROR'
  | 'VALIDATION_ERROR'
  | 'CRITICAL'

class SqlBuilderError extends Error {
  public readonly code: SqlBuilderErrorCode
  public readonly context?: ErrorContext

  constructor(
    message: string,
    code: SqlBuilderErrorCode,
    context?: ErrorContext,
  ) {
    super(message)
    this.name = 'SqlBuilderError'
    this.code = code
    this.context = context
  }
}

export function createError(
  message: string,
  ctx: ErrorContext,
  code: SqlBuilderErrorCode = 'VALIDATION_ERROR',
): SqlBuilderError {
  const parts = [message]

  if (isNonEmptyArray(ctx.path)) {
    parts.push(`Path: ${ctx.path.join('.')}`)
  }

  if (isNotNullish(ctx.modelName)) {
    parts.push(`Model: ${ctx.modelName}`)
  }

  if (isNonEmptyArray(ctx.availableFields)) {
    parts.push(`Available fields: ${ctx.availableFields.join(', ')}`)
  }

  return new SqlBuilderError(parts.join('\n'), code, ctx)
}
