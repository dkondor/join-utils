/*
 * hashjoin.cs -- join two text files using a hashtable, i.e. build a hashtable
 *   from the lines in the first file and probe into that hashtable with fields
 *   from the second file
 * 
 * 	(similar to the join command line utility, but does not need the files
 *  to be sorted)
 * 
 * main motivation is to be able to join text files from the command line
 * without sorting them; this could be useful e.g. when one of the files
 * is too large to be sorted or even generated other than in a streaming
 * fashion, while the other file fits comfortably in memory
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


namespace hashjoin
{
	/* 
	 * utility class to read from a stream while keeping track of the number
	 * of lines read in total
	 * note: we only need the ReadLine() function
	 */
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
	
	/*
	 * utility class to store one line (for the purpose of putting it in a
	 * hashtable), along with an extra bool which keeps track if this line
	 * was matched with at least one line from the second file
	 * main purpose is to allow writing out unmatched files after the end of
	 * the run from the first file as well
	 */
	class File1Line {
		public List<string[]> lines;
		public bool seen;
		public File1Line() { lines = new List<string[]>(); seen = false; }
	}
	
	class MainClass
	{
		public const string usage = @"Usage: hashjoin [OPTION]... FILE1 FILE2
For each pair of input lines with identical join fields, write a line to
standard output.  The default join field is the first, delimited by blanks.

FILE1 is used to build a hashtable first, and then FILE2 is used to probe into
the hashtable and output matching rows. This way, the two files need not be
sorted. Output is written in the same order as read from FILE2.

When FILE1 or FILE2 (not both) is -, read standard input.
  (joining a file that has a literal name of '-' is not supported)

  -a FILENUM        also print unpairable lines from file FILENUM, where
                      FILENUM is 1 or 2, corresponding to FILE1 or FILE2
                      In case of -a 1, unmatched lines from FILE1 are written
                      at the end, i.e. after processing all lines from FILE2
  -e EMPTY          replace missing input fields with EMPTY
  -1 FIELD          join on this FIELD of file 1
  -2 FIELD          join on this FIELD of file 2
  -j FIELD          equivalent to '-1 FIELD -2 FIELD'
  -t CHAR           use CHAR as input and output field separator
  -v FILENUM        like -a FILENUM, but suppress joined output lines
  -o1 FIELDS        output these fields from file 1 (FIELDS is a
                      comma-separated list of field)
  -o2 FIELDS        output these fields from file 2
  -u                allow non-unique join fields from FILE1 (by default multiple
                      occurrences of the same value is treated as an error)
  -H                treat the first line in both files as field headers,
                      print them without trying to pair them
  -h                display this help and exit

Unless -t CHAR is given, leading blanks separate fields and are ignored,
else fields are separated by CHAR.  Any FIELD is a field number counted
from 1.

Important: FILE1 is read first as a whole, and the resulting hashtable has to
fit in the memory. FILE2 is processed in a streaming fashion, so it can be
generated on-the-fly and the size can be indefinite or very large.

";

		/*
		 * reads one line from the given stream and splits it according to the
		 * 	given separator
		 * or returns null on EOF
		 * field is the minimum number of fields required in the line
		 */
		public static string[] ReadLine(StreamLineNum sr, char[] sep, string empty,
				int field) {
			string[] r = null;
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
			return r;
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
			bool unique = true;
			
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
					if( ! (unpaired == 1 || unpaired == 2) ) { Console.Error.WriteLine("-a parameter has to be either 1 or 2\n  use hashjoin -h for help\n"); return; }
					i++;
					break;
				case 'v':
					unpaired = Int32.Parse(args[i+1]);
					if( ! (unpaired == 1 || unpaired == 2) ) { Console.Error.WriteLine("-a parameter has to be either 1 or 2\n  use hashjoin -h for help\n"); return; }
					i++;
					only_unpaired = true;
					break;
				case 'o':
					if(args[i].Length < 3 || !(args[i][2] == '1' || args[i][2] == '2')) { Console.Error.WriteLine("Invalid parameter: {0}\n  (use -o1 or -o2)\n  use hashjoin -h for help\n",args[i]); return; }
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
						if(!valid) { Console.Error.WriteLine("Invalid parameter: {0} {1}\n  use hashjoin -h for help\n",args[i],args[i+1]); return; }
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
				case 'u':
					unique = false;
					break;
				case 'h':
					Console.Write("{0}",usage);
					return;
				default:
					Console.Error.WriteLine("Unknown parameter: {0}\n  use hashjoin -h for help\n");
					return;
			}
			else break; // non-option argument, means the filenames
			// i now points to the first filename
			if(i + 1 >= args.Length) { Console.Error.WriteLine("Error: expecting two input filenames\n  use hashjoin -h for help\n"); return; }
			file1 = args[i];
			file2 = args[i+1];
			if(file1 == file2) { Console.Error.WriteLine("Error: input files have to be different!\n"); return; }
			
			if(field1 < 1 || field2 < 1) { Console.Error.WriteLine("Error: field numbers have to be >= 1!\n"); return; }
			
			if(field1 > req_fields1) req_fields1 = field1;
			if(field2 > req_fields2) req_fields2 = field2;
			
			StreamWriter sw = new StreamWriter(Console.OpenStandardOutput());
			StreamReader sr1 = null;
			StreamReader sr2 = null;
			
			Dictionary<string,File1Line> dict = new Dictionary<string,File1Line>();
			
			string[] file1header = null;
			
			// open input files
			if(file1 == "-") sr1 = new StreamReader(Console.OpenStandardInput());
			else sr1 = new StreamReader(file1);
			if(file2 == "-") sr2 = new StreamReader(Console.OpenStandardInput());
			else sr2 = new StreamReader(file2);
			StreamLineNum s1 = new StreamLineNum(sr1,file1);
			StreamLineNum s2 = new StreamLineNum(sr2,file2);
			
			char out_sep = '\t';
			if(sep != null) out_sep = sep[0];
			
			// read all lines from file 1
			if(header) {
				file1header = ReadLine(s1,sep,empty,req_fields1);
				if(file1header == null) { Console.Error.WriteLine("Error: file 1 is empty (expected header at least!\n"); return; }
			}
			
			while(true) {
				string[] l1 = ReadLine(s1,sep,empty,req_fields1);
				if(l1 == null) break; // end of file
				string key = l1[field1-1]; // note: we already checked that l1 has at least field1 fields in ReadLine()
				if(dict.ContainsKey(key)) {
					if(unique) { Console.Error.WriteLine("Duplicate key in file 1 ({0}): {1} on line {2}!\n",file1,key,s1.Line); return; }
					dict[key].lines.Add(l1);
				}
				else {
					File1Line l2 = new File1Line();
					l2.lines.Add(l1);
					dict.Add(key,l2);
				}
			}
			sr1.Close();
			
			if(header) {
				// read and write output header
				string[] h2 = ReadLine(s2,sep,empty,req_fields2);
				string[] h1 = file1header;
				if(h1 != null && h2 != null) {
					bool firstout = true;
					WriteFields(sw,h1,outfields1,ref firstout,out_sep);
					WriteFields(sw,h2,outfields2,ref firstout,out_sep);
				}
			}
			
			UInt64 out_lines = 0;
			UInt64 matched1 = 0;
			UInt64 matched2 = 0;
			UInt64 unmatched = 0;
			while(true) {
				// read one line from file 2, process it
				string[] line2 = ReadLine(s2,sep,empty,req_fields2);
				if(line2 == null) break;
				string key = line2[field2-1];
				if(dict.ContainsKey(key)) {
					File1Line match = dict[key];
					if(!only_unpaired) {
						if(!match.seen) matched1 += (UInt64)match.lines.Count;
						foreach(string[] line1 in match.lines) {
							bool firstout = true;
							
							// write out fields from the first file
							WriteFields(sw,line1,outfields1,ref firstout,out_sep);
							WriteFields(sw,line2,outfields2,ref firstout,out_sep);
							sw.Write('\n');
							out_lines++;
						}
					}
					match.seen = true;
					matched2++;
				}
				else if(unpaired == 2) {
					// still print unpaired lines from file 2
					bool firstout = true;
					// note: we write empty fields for file 1
					if(outfields1 != null) WriteFields(sw,null,outfields1,ref firstout,out_sep);
					WriteFields(sw,line2,outfields2,ref firstout,out_sep);
					sw.Write('\n');
					out_lines++;
					unmatched++;
				}
			} // main loop
			
			sr2.Close();
			
			// write out unmatched lines from file 1 if needed
			if(unpaired == 1) {
				foreach(File1Line x in dict.Values) if(x.seen == false)
					foreach(string[] line1 in x.lines) {
						// still print unpaired lines from file 1
						bool firstout = true;
						WriteFields(sw,line1,outfields1,ref firstout,out_sep);
						// note: we write empty fields for file 2
						if(outfields2 != null) WriteFields(sw,null,outfields2,ref firstout,out_sep);
						sw.Write('\n');
						out_lines++;
						unmatched++;
				}
			}
			
			
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





