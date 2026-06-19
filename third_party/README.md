**Do not forget to update [LICENSES.md](LICENSES.md)`!**

## iresearch.build

This contains statically generated files for the IResearch folder, and replaces them.

When you update Snowball, you should update `modules.h` in this directory.
Simply run the Perl script in the `snowball` source directory:

```bash
libstemmer/mkmodules.pl temp_modules.h . libstemmer/modules.txt libstemmer/mkinc.mak
```

Then copy `temp_modules.h` to `modules.h`, and fix the paths.

## jemalloc

Only used on Linux/Mac, still uses autofoo.

Updated to the head of the dev branch to make it possible to compile
SereneDB.

The following change has been made to jemalloc compared to upstream commit
e4817c8d89a2a413e835c4adeab5c5c4412f9235:

```diff
diff --git a/3rdParty/jemalloc/jemalloc/src/pages.c b/3rdParty/jemalloc/jemalloc/src/pages.c
index 8cf2fd9f876..11489b3f03d 100644
--- a/3rdParty/jemalloc/jemalloc/src/pages.c
+++ b/3rdParty/jemalloc/jemalloc/src/pages.c
@@ -37,7 +37,7 @@ size_t        os_page;

 #ifndef _WIN32
 #  define PAGES_PROT_COMMIT (PROT_READ | PROT_WRITE)
-#  define PAGES_PROT_DECOMMIT (PROT_NONE)
+#  define PAGES_PROT_DECOMMIT (PROT_READ | PROT_WRITE)
 static int     mmap_flags;
 #endif
 static bool    os_overcommits;
```

## snowball

Don't forget to update `iresearch.build/modules.h`, see [iresearch.build](#iresearchbuild)!

http://snowball.tartarus.org/ stemming for IResearch. We use the latest provided cmake which we maintain.
