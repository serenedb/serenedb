# Generated Snowball benchmark candidates

These files are benchmark-only C candidates for every language covered by
`serenedb-bench-micro-stemming`. They are linked into that benchmark alongside
Serenedb's unmodified `libstemmer_c` implementation.

At benchmark startup, every candidate is checked byte-for-byte against the
Serenedb implementation over the loaded corpus. A mismatching candidate is
reported and is not registered. This keeps the production stemmers unchanged
and makes the baseline/candidate comparison happen in one process.

The files were generated from their corresponding upstream Snowball `.sbl`
rules using the experimental optimized C backend. Common generated-runtime
fast paths apply to all languages; the predicate-aware reverse trie is selected
only for Hindi and Polish. They are temporary evidence for the optimization
work, not a substitute for the planned Serenedb-owned `.sbl` compiler.

The snapshot covers Arabic, English, Finnish, French, German, Greek, Hindi,
Hungarian, Italian, Polish, Russian, Spanish, Tamil, and Turkish. It was
generated with a locally patched compiler based on Snowball commit
`5f0b93ea7353433231dd645a08a865156d27ae49`; the generator patch will replace
these checked-in fixtures once it is ready for review.
