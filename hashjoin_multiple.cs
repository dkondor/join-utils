/*
 * hashjoin_multiple.cs -- join one text file with multiple ones according to
 * 	several fields
 * 
 * e.g. join with file1 using IDs in column 1, with file2 using IDs in column 3,
 * etc.
 * 
 * main motivation is to be able to join a text file with several other sources
 * in one pass and without sorting
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
		public const string usage = @"Usage: hashjoin_mult [OPTIONS] [-i FILE0]
For each input line in FILE0 find matching lines from each of the files
specified as part of OPTIONS and write the combination of all to standard
output. All files given other than FILE0 are read first used to build
hashtables which are used subsequently. If the [-i FILE0] option is omitted,
read from standard input. If no join fields and files are specified, just write
input to standard output (similar to 'cat').

Valid options:

  -v               reverse: only print unmatchable lines from FILE0 (when one
                     or more fields is not found in the match files)
  -m               treat missing join fields as errors instead of ignoring and
                     skipping them (incompatible with the previous option)
  -e EMPTY         replace missing input fields with EMPTY
  -NUM FILE [M]    join field NUM from FILE0 with field M from FILE
                     (defualt for M is 1); one field can only be joined with one
                     file (join the files to be matched first if needed)
  -t CHAR          use CHAR as input and output field separator
  -u               allow non-unique join fields from join FILEs, use only the
                     last value found (by default multiple occurrences of the
                     same value is treated as an error)
  -c               check and require that every row in each file contains the
                     same number of fields (otherwise the output would be
                     probably hard to interpret)
  -H               treat the first line in all files as field headers,
                     print them without trying to pair them
  -h               display this help and exit

Unless -t CHAR is given, leading blanks separate fields and are ignored,
else fields are separated by CHAR.  Any NUM / M is a field number counted
from 1.

Output is in the order of FILE0, all joined fields are inserted after the join
field. The joined fields are not repeated.

Important: the files given as the -NUM FILE option are all read first as a
whole, and the resulting hashtables has to fit in the memory. The main input
FILE is processed in a streaming fashion, so it can be generated on-the-fly
and there is no limit on its size.

Example usage:
  ./hashjoin_mult.exe -i main_data.dat -1 ids1.dat -4 other_data.dat 2
  
    read from main_data.dat, for each line match the first column with the
    first column of ids1.dat and the 4th column with the second column of
    other_data.dat; in the output, contents of ids1.dat will follow the first
    column, and other_data.dat will follow the 4th column from the original
    file; lines which cannot be joined are skipped

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
			string file0 = null;
			Dictionary<int,Tuple<string,int> > matchfiles = new Dictionary<int,Tuple<string,int> >();
			
			char[] sep = (char[])null;
			string empty = null;
			
			bool only_unmatched = false;
			bool skip_missing = true;
			bool header = false;
			bool unique = true;
			bool check_fieldnum = false;
			int req_fields0 = 1;
			
			/* process option arguments */
			for(int i=0;i<args.Length;i++) if(args[i].Length > 1 && args[i][0] == '-') {
				if(char.IsDigit(args[i][1])) {
					/* field to be joined */
					int field;
					if(!Int32.TryParse(args[i].Substring(1),out field) || i+1 == args.Length) {
						Console.Error.WriteLine("Invalid parameter: {0}",args[i]);
						break;
					}
					/* the next argument is treated as a filename regardless of a its format */
					string fn = args[i+1];
					int joinfield = 1;
					if(i+2 < args.Length && args[i+2][0] != '-') {
						i += 2;
						if(!Int32.TryParse(args[i+2],out joinfield)) {
							Console.Error.WriteLine("Invalid parameter: {0} {1} {2}",args[i-2],args[i-1],args[i]);
							break;
						}
					}
					else i++;
					if(matchfiles.ContainsKey(field)) {
						Console.Error.WriteLine("Invalid parameters: join field {0} appears more than once!",field);
						break;
					}
					if(field > req_fields0) req_fields0 = field;
					matchfiles.Add(field,new Tuple<string,int>(fn,joinfield));
				}
				else switch(args[i][1]) {
					case 'v':
						only_unmatched = true;
						break;
					case 'm':
						skip_missing = false;
						break;
					case 't':
						sep = new char[1]{args[i+1][0]};
						i++;
						break;
					case 'e':
						empty = args[i+1];
						i++;
						break;
					case 'H':
						header = true;
						break;
					case 'u':
						unique = false;
						break;
					case 'c':
						check_fieldnum = true;
						break;
					case 'i':
						file0 = args[i+1];
						i++;
						break;
					case 'h':
						Console.Write("{0}",usage);
						return;
					default:
						Console.Error.WriteLine("Unknown parameter: {0}\n  use numjoin -h for help\n");
						return;
				}
			}
			
			/* main data structure to hold the hash tables of the files to be joined */
			List<Tuple<int,Dictionary<string,string[]>,List<int>>> dicts =
				new List<Tuple<int,Dictionary<string,string[]>,List<int>>>();
			/* dict to hold file headers if needed */
			Dictionary<int,Tuple<int,string[]>> headers = new Dictionary<int,Tuple<int,string[]>>();
			/* temporary aggregation so that files that potentially appear multiple times are only read once */
			Dictionary<Tuple<string,int>,List<int>> matchfiles2 = new Dictionary<Tuple<string,int>,List<int>>();
			foreach(var x in matchfiles) {
				if(matchfiles2.ContainsKey(x.Value)) matchfiles2[x.Value].Add(x.Key);
				else matchfiles2.Add(x.Value,new List<int>{x.Key});
			}
			bool firstline;
			/* read each file, create hashtable from the contents */
			foreach(var x in matchfiles2) {
				StreamReader sr1 = new StreamReader(x.Key.Item1);
				StreamLineNum s = new StreamLineNum(sr1,x.Key.Item1);
				int field1 = x.Key.Item2;
				int req_fields = field1;
				Dictionary<string,string[]> d = new Dictionary<string,string[]>();
				if(header) {
					string[] header1 = ReadLine(s,sep,empty,req_fields);
					if(header1 == null) {
						Console.Error.WriteLine("No data read from file {0}!",x.Key.Item1);
						return;
					}
					foreach(var y in x.Value) headers.Add(y,new Tuple<int,string[]>(req_fields,header1));
				}
				firstline = true;
				while(true) {
					string[] l1 = ReadLine(s,sep,empty,req_fields);
					if(check_fieldnum) {
						if(firstline) { req_fields = l1.Length; firstline = false; }
						else if(req_fields != l1.Length) {
							Console.Error.WriteLine("Inconsistent number of fields in file {0} at line {1}!",s.Fn,s.Line);
							return;
						}
					}
					if(l1 == null) break; // end of file
					string key = l1[field1-1]; // note: we already checked that l1 has at least field1 fields in ReadLine()
					if(d.ContainsKey(key)) {
						if(unique) { Console.Error.WriteLine("Duplicate key in file 1 ({0}): {1} on line {2}!\n",s.Fn,key,s.Line); return; }
						d[key] = l1;
					}
					else d.Add(key,l1);
				}
				sr1.Close();
				
				dicts.Add(new Tuple<int,Dictionary<string,string[]>,List<int>>(field1,d,x.Value));
			}
			
			StreamWriter sw = new StreamWriter(Console.OpenStandardOutput());
			StreamReader sr = null;
			StreamLineNum s2 = null;
			if(file0 != null) sr = new StreamReader(file0);
			else sr = new StreamReader(Console.OpenStandardInput());
			{
				string file01 = file0;
				if(file01 == null) file01 = "<stdin>";
				s2 = new StreamLineNum(sr,file01);
			}
			
			char out_sep = '\t';
			if(sep != null) out_sep = sep[0];
			
			if(header) {
				/* read and write output header if requested */
				string[] h2 = ReadLine(s2,sep,empty,req_fields0);
				if(h2 != null) {
					sw.Write(h2[0]);
					for(int i=1;i<h2.Length;i++) {
						Tuple<int,string[]> h1;
						if(only_unmatched == false && headers.TryGetValue(i,out h1)) {
							int key2 = h1.Item1;
							string[] h1s = h1.Item2;
							for(int j=0;j<h1s.Length;j++) if(j+1 != key2) {
								sw.Write(out_sep);
								sw.Write(h1s[j]);
							}
						}
						sw.Write(out_sep);
						sw.Write(h2[i]);
					}
					sw.Write('\n');
				}
			}
			
			UInt64 matched_lines = 0;
			UInt64 unmatched_lines = 0;
			firstline = true;
			/* temporary index to use for matched fields */
			Tuple<int,string[]>[] matched = new Tuple<int,string[]>[req_fields0];
			while(true) {
				// read one line from file0, process it
				string[] line2 = ReadLine(s2,sep,empty,req_fields0);
				if(line2 == null) break;
				if(check_fieldnum) {
					if(firstline) { req_fields0 = line2.Length; firstline = false; }
					else if(req_fields0 != line2.Length) {
						Console.Error.WriteLine("Inconsistent number of fields in file {0} at line {1}!",s2.Fn,s2.Line);
						return;
					}
				}
				
				/* check that all fields to be matched are found */
				bool matched_all = true;
				foreach(var x in dicts) {
					foreach(var y in x.Item3) {
						string key = line2[y-1]; /* key to use for search */
						string[] match;
						if(x.Item2.TryGetValue(key,out match)) {
							matched[y-1] = new Tuple<int,string[]>(x.Item1,match);
						}
						else {
							matched_all = false;
							matched[y-1] = null;
							if(skip_missing == false) {
								Console.Error.WriteLine("Error: key {0} from line {1}, file {2} not found in " +
									"match file {3}!",key,s2.Line,s2.Fn,matchfiles[y].Item1);
							}
							break;
						}
					}
					if(!matched_all) break;
				}
				
				/* main output of results if all were matched */
				if(matched_all) {
					for(int i=0;i<line2.Length;i++) {
						sw.Write(line2[i]);
						if(matched[i] != null) {
							int key2 = matched[i].Item1;
							string[] line1 = matched[i].Item2;
							for(int j=0;j<line1.Length;j++) if(j+1 != key2) {
								sw.Write(out_sep);
								sw.Write(line1[j]);
							}
						}
						if(i+1<line2.Length) sw.Write(out_sep);
					}
					sw.Write('\n');
					matched_lines++;
				}
				else {
					if(skip_missing == false) break;
					if(only_unmatched) {
						/* print the original line */
						sw.Write(line2[0]);
						for(int i=1;i<line2.Length;i++) {
							sw.Write(out_sep);
							sw.Write(line2[i]);
						}
						sw.Write('\n');
					}
					unmatched_lines++;
				}
			} // main loop
			
			sr.Close();
			sw.Close(); // flush output
			
			
			Console.Error.WriteLine("Matched lines: {0}",matched_lines);
			if(skip_missing || only_unmatched) Console.Error.WriteLine("Unmatched lines: {0}",unmatched_lines);
		}
	}
}





