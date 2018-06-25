/*
 * numeric_join.cs -- join two text files by a numeric field
 * 	files must be sorted by that field already
 * 	(similar to the join command line utility, but handles numeric
 * 	join fields instead of strings)
 * 
 * main motivation is to join text files containing numeric fields from the
 * command line without the need to sort them first, which might be
 * impractical if the files are very large and are already sorted by
 * numeric order
 * 
 * Copyright 2018 Daniel Kondor <kondor.dani@gmail.com>
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 * * Neither the name of the  nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 */

using System;
using System.IO;
using System.Collections.Generic;


namespace numeric_join
{
	class StreamLineNum {
		StreamReader sr;
		UInt64 line;
		string fn;
		
		StreamLineNum() { }
		public StreamLineNum(StreamReader sr_, string fn_) { sr = sr_; line = 0; fn = fn_; }
		public UInt64 Line { get { return line; } }
		public string Fn { get { return fn; } }
		public string ReadLine() {
			if(sr.EndOfStream) return null;
			line++;
			return sr.ReadLine();
		}
	}
	
	class MainClass
	{
		public const string usage = @"Usage: numjoin [OPTION]... FILE1 FILE2
For each pair of input lines with identical join fields, write a line to
standard output.  The default join field is the first, delimited by blanks.
The join field has to be an integer and both files need to be sorted on the
join fields (in numeric order).

When FILE1 or FILE2 (not both) is -, read standard input.
  (joining a file that has a literal name of '-' is not supported)

  -a FILENUM        also print unpairable lines from file FILENUM, where
                      FILENUM is 1 or 2, corresponding to FILE1 or FILE2
  -e EMPTY          replace missing input fields with EMPTY
  -1 FIELD          join on this FIELD of file 1
  -2 FIELD          join on this FIELD of file 2
  -j FIELD          equivalent to '-1 FIELD -2 FIELD'
  -t CHAR           use CHAR as input and output field separator
  -v FILENUM        like -a FILENUM, but suppress joined output lines
  -o1 FIELDS        output these fields from file 1 (FIELDS is a
                      comma-separated list of field)
  -o2 FIELDS        output these fields from file 2
  -c                check that the input is correctly sorted, even
                      if all input lines are pairable
  -H                treat the first line in both files as field headers,
                      print them without trying to pair them
  -h                display this help and exit

Unless -t CHAR is given, leading blanks separate fields and are ignored,
else fields are separated by CHAR.  Any FIELD is a field number counted
from 1.

Important: FILE1 and FILE2 must be sorted on the join fields.
  (use sort -n or similar to achieve this)

";

		/*
		 * reads one line from the given stream and splits it according to the
		 * 	given separator
		 * or returns null on EOF
		 * 
		 * throws exception on parse error
		 */
		public static string[] ReadLine(StreamLineNum sr, char[] sep, string empty,
				int field, out Int64 id) {
			string[] r = null;
			id = 0;
			do {
				string line = sr.ReadLine();
				if(line == null) return null;
				if(sep != null) {
					r = line.Split(sep);
					if(empty != null) for(int i=0;i<r.Length;i++)
						if(r[i].Length == 0) r[i] = empty;
				}
				else r = line.Split(sep,StringSplitOptions.RemoveEmptyEntries);
			} while(r.Length == 0);
			
			if(r.Length < field) {
				string err = "Invalid data in input file " + sr.Fn + ", line " +
					sr.Line.ToString() + ": too few fields ( expected at least "
					+ field.ToString() + ", found only " + r.Length + ")!\n";
				Console.Error.WriteLine("{0}",err);
				throw new Exception(err);
			}
			if(field == 0) return r;
			if(!Int64.TryParse(r[field - 1],out id)) {
				string err = "Invalid data in input file " + sr.Fn + ", line " +
					sr.Line.ToString() + ": could not parse " + r[field] +
					" as number!\n";
				Console.Error.WriteLine("{0}",err);
				throw new Exception(err);
			}
			
			return r;
		}
		
		/*
		 * read the next set of lines from the sr
		 * 
		 * input: 
		 * 	 sr -- stream to read
		 *   nextline -- next line in the stream (if already read)
		 *   nextid -- next id in the stream (if already read)
		 *   field -- field containing the ID
		 *   req_fields -- minimum required number of fields
		 *   sep -- separator characters to use
		 *   empty -- replace empty fields with this string
		 * 
		 * output:
		 *   id -- current ID
		 *   lines -- collection of one or more lines with the current ID,
		 *       or empty list if the file is empty
		 *   nextline -- next line in the file (if exists, null if EOF)
		 *   nextid -- next ID in the file (if exists, unchanged otherwise)
		 * 
		 * throws exception on format error
		 */
		public static void ReadNext(StreamLineNum sr, List<string[]> lines,
				ref Int64 id, ref Int64 nextid, ref string[] nextline, int field,
				int req_fields, char[] sep, string empty) {
			lines.Clear();
			if(nextline == null) {
				nextline = ReadLine(sr,sep,empty,field, out nextid);
				if(nextline == null) return; // empty file or end of file
				if(nextline.Length < req_fields) {
					string err = "Invalid data in input file " + sr.Fn +
						", line " + sr.Line.ToString() +
						": too few fields ( expected at least " +
						req_fields.ToString() + ", found only " +
						nextline.Length + ")!\n";
					Console.Error.WriteLine("{0}",err);
					throw new Exception(err);
				}
			}
			id = nextid;
			lines.Add(nextline);
			// read further lines, until we have the same ID in them
			while(true) {
				nextline = ReadLine(sr,sep,empty,field, out nextid);
				if(nextline == null) return; // end of file
				if(nextline.Length < req_fields) {
					string err = "Invalid data in input file " + sr.Fn +
						", line " + sr.Line.ToString() +
						": too few fields ( expected at least " +
						req_fields.ToString() + ", found only " +
						nextline.Length + ")!\n";
					Console.Error.WriteLine("{0}",err);
					throw new Exception(err);
				}
				if(nextid != id) break;
				lines.Add(nextline);
			}
		}
		
		public static void WriteFields(StreamWriter sw, string[] line,
				List<int> fields, ref bool firstout, char out_sep) {
			if(fields != null) foreach(int f in fields) {
				if(firstout) { if(line != null) sw.Write("{0}",line[f-1]); }
				else {
					if(line != null) sw.Write("{0}{1}",out_sep,line[f-1]);
					else sw.Write("{0}",out_sep);
				}
				firstout = false;
			}
			else foreach(string s in line) {
				if(firstout) sw.Write("{0}",s);
				else sw.Write("{0}{1}",out_sep,s);
				firstout = false;
			}
		}

		public static void Main(string[] args) {
			string file1 = null;
			string file2 = null;
			
			int field1 = 1;
			int field2 = 1;
			int req_fields1 = 1;
			int req_fields2 = 1;
			
			List<int> outfields1 = null;
			List<int> outfields2 = null;
			
			char[] sep = (char[])null;
			string empty = null;
			
			int unpaired = 0; // if 1 or 2, print unpaired lines from the given file
			bool only_unpaired = false;
			bool header = false;
			bool strict_order = false;
			
			// process option arguments
			int i=0;
			for(;i<args.Length;i++) if(args[i].Length > 1 && args[i][0] == '-') switch(args[i][1]) {
				case '1':
					field1 = Int32.Parse(args[i+1]);
					i++;
					break;
				case '2':
					field2 = Int32.Parse(args[i+1]);
					i++;
					break;
				case 'j':
					field1 = Int32.Parse(args[i+1]);
					field2 = field1;
					i++;
					break;
				case 't':
					sep = new char[1]{args[i+1][0]};
					i++;
					break;
				case 'e':
					empty = args[i+1];
					i++;
					break;
				case 'a':
					unpaired = Int32.Parse(args[i+1]);
					if( ! (unpaired == 1 || unpaired == 2) ) { Console.Error.WriteLine("-a parameter has to be either 1 or 2\n  use numjoin -h for help\n"); return; }
					i++;
					break;
				case 'v':
					unpaired = Int32.Parse(args[i+1]);
					if( ! (unpaired == 1 || unpaired == 2) ) { Console.Error.WriteLine("-a parameter has to be either 1 or 2\n  use numjoin -h for help\n"); return; }
					i++;
					only_unpaired = true;
					break;
				case 'o':
					if(args[i].Length < 3 || !(args[i][2] == '1' || args[i][2] == '2')) { Console.Error.WriteLine("Invalid parameter: {0}\n  (use -o1 or -o2)\n  use numjoin -h for help\n",args[i]); return; }
					{
						List<int> tmp = new List<int>();
						bool valid = true;
						int max = 0;
						// it is valid to give zero output columns from one of the files
						// (e.g. to filter the other file)
						// this case it might be necessary to give an empty string
						// as the argument (i.e. -o1 "")
						if( !(args[i+1].Length == 0 || args[i+1][0] == '-') ) {
							string[] stmp = args[i+1].Split(',');
							if(stmp.Length == 0) valid = false;
							foreach(string s in stmp) {
								int x;
								if(Int32.TryParse(s,out x)) {
									if(x < 1) { valid = false; break; }
									tmp.Add(x);
									if(x > max) max = x;
								}
								else { valid = false; break; }
							}
						}
						if(!valid) { Console.Error.WriteLine("Invalid parameter: {0} {1}\n  use numjoin -h for help\n",args[i],args[i+1]); return; }
						if(args[i][2] == '1') {
							outfields1 = tmp;
							if(max > req_fields1) req_fields1 = max;
						}
						if(args[i][2] == '2') {
							outfields2 = tmp;
							if(max > req_fields2) req_fields2 = max;
						}
					}
					i++;
					break;
				case 'H':
					header = true;
					break;
				case 'c':
					strict_order = true;
					break;
				case 'h':
					Console.Write("{0}",usage);
					return;
				default:
					Console.Error.WriteLine("Unknown parameter: {0}\n  use numjoin -h for help\n");
					return;
			}
			else break; // non-option argument, means the filenames
			// i now points to the first filename
			if(i + 1 >= args.Length) { Console.Error.WriteLine("Error: expecting two input filenames\n  use numjoin -h for help\n"); return; }
			file1 = args[i];
			file2 = args[i+1];
			if(file1 == file2) { Console.Error.WriteLine("Error: input files have to be different!\n"); return; }
			
			if(field1 < 1 || field2 < 1) { Console.Error.WriteLine("Error: field numbers have to be >= 1!\n"); return; }
			
			StreamWriter sw = new StreamWriter(Console.OpenStandardOutput());
			StreamReader sr1 = null;
			StreamReader sr2 = null;
			
			Int64 id1 = Int64.MinValue;
			Int64 id2 = Int64.MinValue;
			
			List<string[]> lines1 = new List<string[]>();
			string[] next1 = null;
			Int64 nextid1 = Int64.MinValue;
			
			List<string[]> lines2 = new List<string[]>();
			string[] next2 = null;
			Int64 nextid2 = Int64.MinValue;
			
			// open input files
			if(file1 == "-") sr1 = new StreamReader(Console.OpenStandardInput());
			else sr1 = new StreamReader(file1);
			if(file2 == "-") sr2 = new StreamReader(Console.OpenStandardInput());
			else sr2 = new StreamReader(file2);
			StreamLineNum s1 = new StreamLineNum(sr1,file1);
			StreamLineNum s2 = new StreamLineNum(sr2,file2);
			
			char out_sep = '\t';
			if(sep != null) out_sep = sep[0];
			
			if(header) {
				// read and write output header
				Int64 tmp;
				string[] h1 = ReadLine(s1,sep,empty,0,out tmp);
				string[] h2 = ReadLine(s2,sep,empty,0,out tmp);
				if(h1 != null && h2 != null) {
					if(h1.Length < req_fields1) { Console.Error.WriteLine("Header too short in file 1!\n"); return; }
					if(h2.Length < req_fields2) { Console.Error.WriteLine("Header too short in file 2!\n"); return; }
					
					bool firstout = true;
					WriteFields(sw,h1,outfields1,ref firstout,out_sep);
					WriteFields(sw,h2,outfields2,ref firstout,out_sep);
				}
			}
			
			// read first lines
			ReadNext(s1,lines1,ref id1,ref nextid1,ref next1,field1,req_fields1,sep,empty);
			ReadNext(s2,lines2,ref id2,ref nextid2,ref next2,field2,req_fields2,sep,empty);
			
			UInt64 out_lines = 0;
			UInt64 matched1 = 0;
			UInt64 matched2 = 0;
			UInt64 unmatched = 0;
			while(true) {
				if(lines1.Count == 0 && lines2.Count == 0) break; // end of both files
				if(lines1.Count == 0 && unpaired != 2) break;
				if(lines2.Count == 0 && unpaired != 1) break;
				if(lines1.Count > 0 && lines2.Count > 0 && id1 == id2) {
					// match, write out (if needed -- not only_unpaired)
					// there could be several lines from both files, iterate
					// over the cross product
					if(!only_unpaired) {
						matched1 += (UInt64)lines1.Count;
						matched2 += (UInt64)lines2.Count;
						foreach(string[] line1 in lines1)
						foreach(string[] line2 in lines2) {
							bool firstout = true;
							
							// write out fields from the first file
							WriteFields(sw,line1,outfields1,ref firstout,out_sep);
							WriteFields(sw,line2,outfields2,ref firstout,out_sep);
							sw.Write('\n');
							out_lines++;
						}
					}
					
					if(strict_order) {
						// check order
						if(next1 != null && nextid1 < id1) {
							string err = "Error: input file " + s1.Fn +
								" not sorted on line " + s1.Line + " ( " +
								nextid1.ToString() + " < " + id1.ToString() + ")!\n";
							Console.Error.WriteLine("{0}",err);
							break;
						}
						if(next2 != null && nextid2 < id2) {
							string err = "Error: input file " + s2.Fn +
								" not sorted on line " + s2.Line + " ( " +
								nextid2.ToString() + " < " + id2.ToString() + ")!\n";
							Console.Error.WriteLine("{0}",err);
							break;
						}
					}
					
					// read next lines
					ReadNext(s1,lines1,ref id1,ref nextid1,ref next1,field1,req_fields1,sep,empty);
					ReadNext(s2,lines2,ref id2,ref nextid2,ref next2,field2,req_fields2,sep,empty);
					continue; // skip following section, the next lines might be a match as well
				} // write out one match
				
				// no match
				if(lines1.Count > 0 && (id1 < id2 || lines2.Count == 0) ) {
					// need to advance file1
					
					// check if lines from file 1 should be output if not matched
					if(unpaired == 1) foreach(string[] line1 in lines1) {
						// still print unpaired lines from file 1
						bool firstout = true;
						WriteFields(sw,line1,outfields1,ref firstout,out_sep);
						// note: we write empty fields for file 2
						if(outfields2 != null) WriteFields(sw,null,outfields2,ref firstout,out_sep);
						sw.Write('\n');
						out_lines++;
						unmatched++;
					}
					
					// first check sort order, that could be a problem here
					if(next1 != null && nextid1 < id1) {
						string err = "Error: input file " + s1.Fn +
							" not sorted on line " + s1.Line + " ( " +
							nextid1.ToString() + " < " + id1.ToString() + ")!\n";
						Console.Error.WriteLine("{0}",err);
						break;
					}
					
					ReadNext(s1,lines1,ref id1,ref nextid1,ref next1,field1,req_fields1,sep,empty);
				}
				else {
					// here id2 < id1 or lines1.Count == 0 and lines2.Count > 0
					// check if lines from file 2 should be output if not matched
					if(unpaired == 2) foreach(string[] line2 in lines2) {
						// still print unpaired lines from file 2
						bool firstout = true;
						// note: we write empty fields for file 1
						if(outfields1 != null) WriteFields(sw,null,outfields1,ref firstout,out_sep);
						WriteFields(sw,line2,outfields2,ref firstout,out_sep);
						sw.Write('\n');
						out_lines++;
						unmatched++;
					}
					
					// first check sort order, that could be a problem here
					if(next2 != null && nextid2 < id2) {
						string err = "Error: input file " + s2.Fn +
							" not sorted on line " + s2.Line + " ( " +
							nextid2.ToString() + " < " + id2.ToString() + ")!\n";
						Console.Error.WriteLine("{0}",err);
						break;
					}
					
					ReadNext(s2,lines2,ref id2,ref nextid2,ref next2,field2,req_fields2,sep,empty);
				}
				
			} // main loop
			
			sr1.Close();
			sr2.Close();
			sw.Close(); // flush output
			
			Console.Error.WriteLine("Matched lines from file 1: {0}",matched1);
			Console.Error.WriteLine("Matched lines from file 2: {0}",matched2);
			if(unmatched > 0) switch(unpaired) {
				case 1:
					Console.Error.WriteLine("Unmatched lines from file 1: {0}",unmatched);
					break;
				case 2:
					Console.Error.WriteLine("Unmatched lines from file 2: {0}",unmatched);
					break;
			}
			Console.Error.WriteLine("Total lines output: {0}",out_lines);
		}
	}
}





