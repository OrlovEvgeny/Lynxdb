/**
 * Shared API response types.
 * This module is the canonical location for TypeScript interfaces
 * used across the API layer.
 */

/** Standard error shape returned by LynxDB API. */
export interface APIError {
  code: string;
  message: string;
  suggestion?: string;
}
