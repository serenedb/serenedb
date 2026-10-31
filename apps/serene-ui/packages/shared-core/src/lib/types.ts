/**
 * Converts a union type to an intersection type.
 *
 * @example
 * type Union = { a: string } | { b: number };
 * type Result = UnionToIntersection<Union>;
 * // Result: { a: string } & { b: number }
 *
 * This is achieved using distributive conditional types and contravariance in function parameters.
 */
export type UnionToIntersection<U> = (
    U extends any ? (k: U) => void : never
) extends (k: infer I) => void
    ? I
    : never;

/**
 * Flattens a union type into a single object type with all properties optional.
 *
 * @example
 * type Union = { a: string } | { b: number };
 * type Result = FlattenUnionOptional<Union>;
 * // Result: { a?: string; b?: number }
 *
 * All keys from the union are collected and made optional since not all union members
 * contain every key.
 */
export type FlattenUnionOptional<U> = {
    [K in U extends any ? keyof U : never]?: U extends Record<K, infer V>
        ? V
        : never;
};

/**
 * Flattens a union type into a single object type by converting it to an intersection first.
 *
 * @example
 * type Union = { a: string } | { b: number };
 * type Result = FlattenUnionStrict<Union>;
 * // Result: { a: string; b: number }
 *
 * Useful for merging union types into a single, flat object structure where all
 * properties are required.
 */
export type FlattenUnionStrict<U> = {
    [K in keyof UnionToIntersection<U>]: UnionToIntersection<U>[K];
};

/**
 * Makes all properties of a type nullable (can be null).
 *
 * @example
 * type User = { name: string; age: number };
 * type Result = Nullable<User>;
 * // Result: { name: string | null; age: number | null }
 *
 * This is useful for database rows where columns can be NULL.
 */
export type Nullable<T> = { [K in keyof T]: T[K] | null };

/**
 * Converts JSON-serializable fields (arrays and objects) to strings for database storage.
 *
 * @example
 * type Entity = {
 *   id: number;
 *   data: Record<string, any>[];
 *   metadata?: Record<string, any>;
 * };
 * type Row = JsonFieldsToString<Entity>;
 * // Result: { id: number; data: string; metadata?: string | undefined }
 *
 * SQLite and most SQL databases don't support native array or object types.
 * Complex data structures must be JSON stringified before storage and parsed after retrieval.
 * This type automatically converts all arrays and Record types to strings while preserving
 * their optionality (undefined) and nullability (null).
 */
export type JsonFieldsToString<T> = {
    [K in keyof T]: T[K] extends Array<any>
        ? T[K] extends undefined
            ? string | undefined
            : string
        : T[K] extends Array<any> | undefined
          ? string | undefined
          : T[K] extends Array<any> | null
            ? string | null
            : T[K] extends Record<string, any>
              ? T[K] extends undefined
                  ? string | undefined
                  : string
              : T[K] extends Record<string, any> | undefined
                ? string | undefined
                : T[K] extends Record<string, any> | null
                  ? string | null
                  : T[K];
};

/**
 * Combines JsonFieldsToString, FlattenUnion, and Nullable to create a type suitable for database rows.
 *
 * @example
 * type Entity = {
 *   id: number;
 *   data: Record<string, any>[];
 *   metadata?: Record<string, any>;
 * } | {
 *   id: number;
 *   info: string[];
 *   details?: Record<string, any>;
 * };
 * type Row = ToRow<Entity>;
 * // Result: { id: number; data: string | null; metadata?: string | undefined | null; info: string | null; details?: string | undefined | null }
 *
 * This type is useful for defining the shape of database rows where:
 * - All JSON-serializable fields are converted to strings.
 * - The union types are flattened into a single object type.
 * - All properties are made nullable to accommodate SQL NULL values.
 */
export type ToRow<T> = Nullable<FlattenUnionStrict<JsonFieldsToString<T>>>;
