// Copyright (c) 2014 Philip M. Hubbard
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// 
// http://opensource.org/licenses/MIT

package com.philiphubbard.sabe;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException; 
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

// A sample driver application for running the MRAssembler class with Hadoop.
// The test data in this case is a larger set of reads, containing errors and
// repetitive sections repeated once ("single repeats"), and a goal that is 
// a longer sequence.

public class MRAssemblerTest2 {
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		setupTest(conf);
		
		MRAssembler assembler = new MRAssembler(MER_LENGTH, COVERAGE);
		assembler.run(new Path(testInput), new Path(testOutput));
		
		verifyTest(conf);
		cleanupTest(conf);
		
		System.exit(0);
	}
	
	private static void setupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		
		Path path = new Path(testInput);
		if (fileSystem.exists(path))
			fileSystem.delete(path, true);
		
		ArrayList<Text> reads = new ArrayList<Text>();
		
		// The expected result:
		// CCCTTTCTGTTGACCCATCATTGTTTAGTAACCCGCGGGATGCCTGGCAGACCCGCGGGACGATCTCCTCTGACCCATCATCGAAATTCC
		// Note that it has the following pattern:
		// segment 0: CCCTTTCTGT 
		// segment 1, which will be repeated: TGACCCATCA 
		// segment 2: TTGTTTAGTA 
		// segment 3, which will be repeated: ACCCGCGGGA 
		// segment 4: TGCCTGGCAG 
		// segment 3, again: ACCCGCGGGA 
		// segment 5: CGATCTCCTC
		// segment 1, again: TGACCCATCA 
		// segment 6: TCGAAATTCC

		reads.add(new Text("CCCTTTC\n"));
		// Error: initial T omitted.
		reads.add(new Text("GTTGACCCATCATTGTTTAGTAACCCGCGGGATGCCTGGCAGACC"));
		reads.add(new Text("CGCGGGACGAT\n"));
		// Error: final C omitted.
		reads.add(new Text("CTCCTCTGACCCATCATCGAAATTC\n"));

		reads.add(new Text("CCCTTTCTGTTGACCCAT\n"));
		// Error: final C replaced with G.
		reads.add(new Text("CATTGTTTAGTAACCCGCGGGATGCCTGGCAGACG\n"));
		reads.add(new Text("CGCGGGACGATCTCCTCTGACCCATCATCGAAATTCC\n"));

		// Error: C at index 14 replaced with A.
		reads.add(new Text("CCCTTTCTGTTGACACATCATTGTTTAGTAAC"));
		reads.add(new Text("CCGCGGGATGCC\n"));
		// Error: C at index 25 omitted.
		reads.add(new Text("TGGCAGACCCGCGGGACGATCTCCTTGACCCATCATCGAAATTCC\n"));

		reads.add(new Text("CCCTTTCTGTTGACCCATCATTGTTTAGTAACCCGCGGGATGCCTG\n"));
		// Error: G at index 10 replaced with T.
		reads.add(new Text("GCAGACCCGCTGGACGA\n"));
		reads.add(new Text("TCTCCTCTGACCCATCATCGAAATTCC\n"));

		reads.add(new Text("CCCTTTCTGTTGACCCATCATTGTTTAGTAACCCGCGGGATGC"));
		// Error: final G omitted.
		reads.add(new Text("CTGGCAGACCCGC\n"));
		reads.add(new Text("GGACGATCTCCTCT\n"));
		// Error: CG at index 10 transposed to GC
		reads.add(new Text("GACCCATCATCGAAATTCC\n"));

		FSDataOutputStream out = fileSystem.create(path);
		for (Text read : reads) {
			byte[] bytes = read.copyBytes();
			for (byte b : bytes)
				out.write(b);
		}
		out.close();
		
		fileSystem.close();
	}

	private static void verifyTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		FSDataInputStream output = fileSystem.open(new Path(testOutput));
		BufferedReader reader = new BufferedReader(new InputStreamReader(output));

		String actual = reader.readLine();
		final String expected = 
				"CCCTTTCTGTTGACCCATCATTGTTTAGTAACCCGCGGGATGCCTGGCAGACCCGCGGGACGATCTCCTCTGACCCATCATCGAAATTCC";
		
		System.out.println(expected);
		
		if (!actual.equals(expected))
			throw new IOException("Test failed with incorrect result:\n" + actual);
		
		reader.close();
		
		System.out.println("Test succeeded.");
	}

	private static void cleanupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		
		fileSystem.delete(new Path(testInput), true);
		fileSystem.delete(new Path(testOutput), true);
		
		fileSystem.close();
	}
	
	private static final int MER_LENGTH = 6;
	private static final int COVERAGE = 5;

	private static String testInput = new String("MRAssemblerTest_in.txt");
	private static String testOutput = new String("MRAssemblerTest_out");
}
