# join-utils
Command line utilities for joining text files similary to the UNIX join program with some extra functionality

This repository contains 3 different variations on the
[UNIX / GNU 'join' command](https://www.gnu.org/software/coreutils/manual/html_node/join-invocation.html#join-invocation)
written in C# with the following additional functionality:

- numeric_join.cs: the join field is considered to be numeric (a 64-bit signed integer) and files are expected to be sorted in numeric order;
this is supposed to save the extra sort step as the original join command only expects files sorted in dictionary order

- hashjoin.cs: instead of requiring sorted input, it uses a hashtable to join files; the hashtable is built from the first file (so that has
to be of moderate size), and the second file is processed in a streaming fashion; useful if one of the files is very large

- hashjoin_multiple.cs: similar to the previous, but multiple hashtables can be built from multiple files to perform several join steps in one pass


All were tested on Linux using the Microsoft csc compiler (version 2.6), and the Mono runtime (version 5.10). Compiling should be straightforward from
the command line using the csc command (e.g. 'csc hashjoin.cs'); for Visual Studio, just create an empty project and add the corresponding file.
All programs have a short description in the source and display usage instructions with the '-h' command line option. Most options follow those of
the original 'join' command, where possible.


