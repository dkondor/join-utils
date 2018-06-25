/*
 * hashjoin.cpp -- join two text files using a hashtable, i.e. build a hashtable
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
 * ported to C++ from C# for better portability and performance
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


#include <iostream>
#include <vector>
#include <stdlib.h>
#include <string.h>
#include <string>
//~ #include <random>
#include <unordered_map>
#include "read_table_cpp.h"



/*-----------------------------------------------------------------------------
 * Murmurhash for strings since C++ hash functions only support std::string
 * slightly modified from
 * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp
 * MurmurHash2, 64-bit versions, by Austin Appleby
 * 64-bit hash for 64-bit platforms
 * 
 * MurmurHash2 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this source code.
*/
uint64_t MurmurHash64A ( const char * key, size_t len, uint64_t seed )
{
	const uint64_t m = 0xc6a4a7935bd1e995UL;
	const int r = 47;
	
	uint64_t h = seed ^ (len * m);
	
	while(len >= 8)
	{
		/* note: use memcpy() to avoid UB from strict aliasing violation
		 * should be compiled to a single load instruction */
		uint64_t k;
		memcpy(&k,key,8);
		key += 8;
		len -= 8;
		
		k *= m; 
		k ^= k >> r; 
		k *= m; 
		
		h ^= k;
		h *= m; 
	}
	
	switch(len)
	{
		case 7: h ^= uint64_t(key[6]) << 48;
		case 6: h ^= uint64_t(key[5]) << 40;
		case 5: h ^= uint64_t(key[4]) << 32;
		case 4: h ^= uint64_t(key[3]) << 24;
		case 3: h ^= uint64_t(key[2]) << 16;
		case 2: h ^= uint64_t(key[1]) << 8;
		case 1: h ^= uint64_t(key[0]); h *= m;
	};
	
	h ^= h >> r;
	h *= m;
	h ^= h >> r;
	
	return h;
}

struct string_view_custom_hash {
	uint64_t seed;
	string_view_custom_hash():seed(0xe6573480bcc4fceaUL) {  }
	string_view_custom_hash(uint64_t seed_):seed(seed_) {  }
	size_t operator () (const string_view_custom& s) const {
		return MurmurHash64A(s.data(),s.length(),seed);
	}
};


/*
 * utility class to store one line (for the purpose of putting it in a
 * hashtable), along with an extra bool which keeps track if this line
 * was matched with at least one line from the second file
 * main purpose is to allow writing out unmatched files after the end of
 * the run from the first file as well
 */
struct File1Line {
	std::vector<std::pair<char*,std::vector<string_view_custom> > > lines;
	bool seen;
	File1Line():seen(false) {  }
	~File1Line() {
		for(auto& p : lines) free(p.first);
	}
	File1Line(const File1Line&) = delete; /* it's an error to copy */
	File1Line(File1Line&& f):seen(f.seen) { lines.swap(f.lines); }
	File1Line& operator = (const File1Line&) = delete;
	File1Line& operator = (File1Line&& f) { seen = f.seen; lines.swap(f.lines); return *this; }
};


const char usage[] = R"!!!(Usage: hashjoin [OPTION]... FILE1 FILE2
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
  -s NUM            use NUM as salt when computing hash of strings
  -h                display this help and exit

Unless -t CHAR is given, leading blanks separate fields and are ignored,
else fields are separated by CHAR.  Any FIELD is a field number counted
from 1.

Important: FILE1 is read first as a whole, and the resulting hashtable has to
fit in the memory. FILE2 is processed in a streaming fashion, so it can be
generated on-the-fly and the size can be indefinite or very large.

)!!!";



/*
 * reads one line from the given stream and splits it to string fields
 * returns true on success, false on EOF or parse error
 * field is the number of fields required in the line
 * 
 * if successful (true was returned), the caller has to free() res.first later
 * on error or EOF (false is returned), res.first is not set, it should not be used by the caller
 */
bool ParseLine(line_parser& sr, std::vector<string_view_custom>& res) {
	if(res.empty()) while(true) {
		string_view_custom s;
		if(!sr.read_string_view_custom(s)) return sr.get_last_error() == T_EOL;
		res.push_back(s);
	}
	else for(string_view_custom& s : res) if(!sr.read_string_view_custom(s)) return false;
	return true;
}
bool ReadLine(read_table2& sr, size_t field, std::pair<char*,std::vector<string_view_custom> >& res) {
	if(!sr.read_line()) {
		if(sr.get_last_error() != T_EOF) sr.write_error(std::cerr);
		return false;
	}
	
	res.second.clear();
	res.second.resize(field);
	if(!ParseLine(sr,res.second)) {
		std::cerr<<"ReadLine: ";
		sr.write_error(std::cerr);
		return false;
	}
	/* copy line, update pointers */
	const std::string& buf = sr.get_line_str();
	char* tmp = (char*)malloc(sizeof(char)*(buf.length()+1));
	if(!tmp) return false;
	memcpy(tmp,buf.data(),buf.length());
	tmp[buf.length()] = 0;
	for(string_view_custom& p : res.second) {
		/* adjust pointers to tmp */
		std::ptrdiff_t diff = p.str - buf.data();
		p.str = tmp + diff;
	}
	res.first = tmp;
	return true;
}

template<class string_type>
static void WriteFields(std::ostream& sw, const std::vector<string_type>& line,
		const std::vector<int>& fields, bool& firstout, char out_sep) {
	if(!fields.empty()) for(int f : fields) {
		if(firstout) { if(!line.empty()) sw<<line[f-1]; }
		else {
			if(!line.empty()) sw<<out_sep<<line[f-1];
			else sw<<out_sep;
		}
		firstout = false;
	}
	else for(auto& s : line) {
		if(firstout) sw<<s;
		else sw<<out_sep<<s;
		firstout = false;
	}
}

int main(int argc, char** args) {
	const char* file1 = 0;
	const char* file2 = 0;
	
	int field1 = 1;
	int field2 = 1;
	int req_fields1 = 1;
	int req_fields2 = 1;
	
	std::vector<int> outfields1;
	std::vector<int> outfields2;
	
	char delim = 0;
	char comment = 0;
	//~ string empty = null;
	
	int unpaired = 0; // if 1 or 2, print unpaired lines from the given file
	bool only_unpaired = false;
	bool header = false;
	bool unique = true;
	uint64_t seed;
	bool use_seed;
	
	// process option arguments
	int i=1;
	for(;i<argc;i++) if(args[i][0] == '-' && args[i][1] != 0) switch(args[i][1]) {
		case '1':
			field1 = atoi(args[i+1]);
			i++;
			break;
		case '2':
			field2 = atoi(args[i+1]);
			i++;
			break;
		case 'j':
			field1 = atoi(args[i+1]);
			field2 = field1;
			i++;
			break;
		case 't':
			delim = args[i+1][0];
			i++;
			break;
		case 'C':
			comment = args[i+1][0];
			i++;
			break;
/*		case 'e':
			empty = args[i+1];
			i++;
			break; */
		case 'a':
			unpaired = atoi(args[i+1]);
			if( ! (unpaired == 1 || unpaired == 2) ) { std::cerr<<"-a parameter has to be either 1 or 2\n  use hashjoin -h for help\n"; return 1; }
			i++;
			break;
		case 'v':
			unpaired = atoi(args[i+1]);
			if( ! (unpaired == 1 || unpaired == 2) ) { std::cerr<<"-a parameter has to be either 1 or 2\n  use hashjoin -h for help\n"; return 1; }
			i++;
			only_unpaired = true;
			break;
		case 'o':
			if(!(args[i][2] == '1' || args[i][2] == '2')) { std::cerr<<"Invalid parameter: "<<args[i]<<"\n  (use -o1 or -o2)\n  use hashjoin -h for help\n"; return 1; }
			{
				std::vector<int> tmp;
				bool valid = true;
				int max = 0;
				// it is valid to give zero output columns from one of the files
				// (e.g. to filter the other file)
				// this case it might be necessary to give an empty string
				// as the argument (i.e. -o1 "")
				if( !(args[i+1][0] == 0 || args[i+1][0] == '-') ) {
					line_parser lp(line_parser_params().set_delim(','),args[i+1]);
					int x;
					while(lp.read(x)) { tmp.push_back(x); if(x > max) max = x; if(x < 1) { valid = false; break; } }
					if(lp.get_last_error() != T_EOL || tmp.empty()) valid = false;
				}
				if(!valid) { std::cerr<<"Invalid parameter: "<<args[i]<<" "<<args[i+1]<<"\n  use hashjoin -h for help\n"; return 1; }
				if(args[i][2] == '1') {
					outfields1 = std::move(tmp);
					if(max > req_fields1) req_fields1 = max;
				}
				if(args[i][2] == '2') {
					outfields2 = std::move(tmp);
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
		case 's':
			seed = strtoul(args[i+1],0,10);
			use_seed = true;
			break;
		case 'h':
			std::cout<<usage;
			return 0;
		default:
			std::cerr<<"Unknown parameter: "<<args[i]<<"\n  use hashjoin -h for help\n";
			return 1;
	}
	else break; // non-option argument, means the filenames
	// i now points to the first filename
	if(i + 1 >= argc) { std::cerr<<"Error: expecting two input filenames\n  use hashjoin -h for help\n"; return 1; }
	file1 = args[i];
	file2 = args[i+1];
	if(!strcmp(file1,file2)) { std::cerr<<"Error: input files have to be different!\n"; return 1; }
	if(file1[0] == '-' && file1[1] == 0) file1 = 0;
	if(file2[0] == '-' && file2[1] == 0) file2 = 0;
	if(field1 < 1 || field2 < 1) { std::cerr<<"Error: field numbers have to be >= 1!\n"; return 1; }
	
	if(field1 > req_fields1) req_fields1 = field1;
	if(field2 > req_fields2) req_fields2 = field2;
	
	// open input files + set output stream
	auto& sw = std::cout;
	read_table2 s1(file1,std::cin,line_parser_params().set_delim(delim).set_comment(comment));
	read_table2 s2(file2,std::cin,line_parser_params().set_delim(delim).set_comment(comment));
	
	string_view_custom_hash hash;
	if(use_seed) hash = string_view_custom_hash(seed);
	else hash = string_view_custom_hash();
	std::unordered_map<string_view_custom,File1Line,string_view_custom_hash> dict(0,hash);
	
	std::vector<std::string> file1header;
	
	char out_sep = '\t';
	if(delim) out_sep = delim;
	
	// read all lines from file 1
	if(header) {
		if(!s1.read_line()) { std::cerr<<"Error reading header from file 1:\n"; s1.write_error(std::cerr); return 1; }
		for(int j=0;j<req_fields1;j++) {
			std::string tmp;
			if(!s1.read_string(tmp)) { std::cerr<<"Error reading header from file 1:\n"; s1.write_error(std::cerr); return 1; }
			file1header.push_back(std::move(tmp));
		}
	}
	
	bool firstline = true;
	while(true) {
		std::pair<char*,std::vector<string_view_custom> > tmp;
		size_t read_fields = req_fields1;
		if(outfields1.empty()) read_fields = 0;
		if(!ReadLine(s1,read_fields,tmp)) break; /* end of file or error */
		if(!read_fields) {
			if(tmp.second.size() < field1) {
				std::cerr<<"Too few fields in file 1 ("<<(file1?file1:"<stdin>")<<"), line "<<s1.get_line()<<"!\n";
				return 1;
			}
		}
		const string_view_custom& key = tmp.second[field1-1];
		if(unique) {
			auto it = dict.find(key);
			if(it != dict.end()) {
				std::cerr<<"Duplicate key in file 1 ("<<(file1?file1:"<stdin>")<<"): "<<key<<" on line "<<s1.get_line()<<"!\n";
				return 1;
			}
		}
		dict[key].lines.push_back(std::move(tmp)); /* will add new item if key is not found */
	}
	if(s1.get_last_error() != T_EOF) return 1;
	
	
	if(header) {
		// read and write output header
		if(!s2.read_line()) { std::cerr<<"Error reading header from file 2:\n"; s2.write_error(std::cerr); return 1; }
		std::vector<std::string> file2header;
		for(int j=0;j<req_fields2;j++) {
			std::string tmp;
			if(!s2.read_string(tmp)) { std::cerr<<"Error reading header from file 1:\n"; s2.write_error(std::cerr); return 1; }
			file2header.push_back(std::move(tmp));
		}
		bool firstout = true;
		WriteFields(sw,file1header,outfields1,firstout,out_sep);
		WriteFields(sw,file2header,outfields2,firstout,out_sep);
	}
	
	uint64_t out_lines = 0;
	uint64_t matched1 = 0;
	uint64_t matched2 = 0;
	uint64_t unmatched = 0;
	std::vector<string_view_custom> line2(req_fields2);
	while(true) {
		// read one line from file 2, process it
		if(!s2.read_line()) {
			if(s2.get_last_error() != T_EOF) s2.write_error(std::cerr);
			break;
		}
		if(outfields2.empty()) line2.clear();
		if(!ParseLine(s2,line2)) {
			s2.write_error(std::cerr);
			break;
		}
		if(outfields2.empty() && line2.size() < field2)  {
			std::cerr<<"Too few fields in file 2 ("<<(file2?file2:"<stdin>")<<"), line "<<s2.get_line()<<"!\n";
			break;
		}
		const string_view_custom& key = line2[field2-1];
		auto it = dict.find(key);
		if(it != dict.end()) {
			File1Line& match = it->second;
			if(!only_unpaired) {
				if(!match.seen) matched1 += match.lines.size();
				for(const auto& line1 : match.lines) {
					bool firstout = true;
					
					// write out fields from the first file
					WriteFields(sw,line1.second,outfields1,firstout,out_sep);
					WriteFields(sw,line2,outfields2,firstout,out_sep);
					sw<<'\n';
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
			if(!outfields1.empty()) WriteFields(sw,std::vector<string_view_custom>(),outfields1,firstout,out_sep);
			WriteFields(sw,line2,outfields2,firstout,out_sep);
			sw<<'\n';
			out_lines++;
			unmatched++;
		}
	} // main loop
	
	// write out unmatched lines from file 1 if needed
	if(unpaired == 1) {
		for(const auto& x : dict) if(x.second.seen == false)
			for(const auto& line1 : x.second.lines) {
				// still print unpaired lines from file 1
				bool firstout = true;
				WriteFields(sw,line1.second,outfields1,firstout,out_sep);
				// note: we write empty fields for file 2
				if(!outfields2.empty()) WriteFields(sw,std::vector<string_view_custom>(),outfields2,firstout,out_sep);
				sw<<'\n';
				out_lines++;
				unmatched++;
		}
	}
	
	
	sw.flush(); // flush output
	
	std::cerr<<"Matched lines from file 1: "<<matched1<<'\n';
	std::cerr<<"Matched lines from file 2: "<<matched2<<'\n';
	if(unmatched > 0) switch(unpaired) {
		case 1:
			std::cerr<<"Unmatched lines from file 1: "<<unmatched<<'\n';
			break;
		case 2:
			std::cerr<<"Unmatched lines from file 2: "<<unmatched<<'\n';
			break;
	}
	std::cerr<<"Total lines output: "<<out_lines<<'\n';
}



