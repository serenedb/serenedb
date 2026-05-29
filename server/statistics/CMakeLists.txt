# statistics dir is now header-only (no-op stubs for {connection,request}_statistics.h).
# The ArangoDB statistics framework was deleted in the cleanup; the
# headers survive so *_comm_task / RestHandler compile without churn
# until those paths are cleaned up.
