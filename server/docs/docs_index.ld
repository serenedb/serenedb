SECTIONS
{
  .sdb_docs : { sdb_docs_region = .; KEEP(*(.sdb_docs)) }
}
INSERT AFTER .bss;
