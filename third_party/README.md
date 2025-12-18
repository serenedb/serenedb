**Do not forget to update [LICENSES.md](LICENSES.md)`!**

## date

Forward port of C++20 date/time class. We have patched this slightly to
avoid a totally unnecessary user database lookup which poses problems
with static glibc builds and nsswitch.

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

## linenoise-ng

Our maintained fork of linenoise
https://github.com/antirez/linenoise

We may want to switch to replxx (uniting our & other forks):
https://github.com/AmokHuginnsson/replxx

## snowball

Don't forget to update `iresearch.build/modules.h`, see [iresearch.build](#iresearchbuild)!

http://snowball.tartarus.org/ stemming for IResearch. We use the latest provided cmake which we maintain.

## tzdata

IANA time zone database / Olson database
Contains information about the world's time zones

Upstream is: https://www.iana.org/time-zones (Data Only Distribution)

Windows builds require windowsZones.xml from the Unicode CLDR project:
https://github.com/unicode-org/cldr/blob/master/common/supplemental/windowsZones.xml

invoke `Installation/fetch_tz_database.sh` to do this.
Fix CMakeLists.txt with new zone files if neccessary.

## V8

Javascript interpreter.
This is maintained via https://github.com/serenedb/v8

Upstream is: https://chromium.googlesource.com/v8/v8.git

- On upgrade the ICU data file(s) need to be replaced with ICU upstream,
  since the V8 copy doesn't contain all locales (known as full-icu, ~25 MB icudt*.dat).

## ZLib

ZLib compression library https://zlib.net/

Compared to the original zlib 1.2.13, we have made changes to the CMakeLists.txt
as can be found in the [patch file](zlib/zlib-1.2.13.patch)
