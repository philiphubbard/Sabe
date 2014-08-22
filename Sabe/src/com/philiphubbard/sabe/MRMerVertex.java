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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

import com.philiphubbard.digraph.MRVertex;

public class MRMerVertex extends MRVertex {
	
	//
	
	public static final String CONFIG_MER_LENGTH = "CONFIG_MER_LENGTH";
	
	public MRMerVertex(int id, Configuration config) {
		super(id, config);
		merString = new MerString(id, config.getInt(CONFIG_MER_LENGTH, 1));
	}
	
	public MRMerVertex(BytesWritable writable, Configuration config) {
		super(writable, config);
	}
	
	public MerString getMerString() {
		return merString;
	}
	
	public boolean equals(MRMerVertex other) {
		if (!super.equals(other))
			return false;
		return (merString.equals(other.merString));
	}
	
	public String toDisplayString(Configuration config) {
		StringBuilder s = new StringBuilder();
		
		s.append("MRMerVertex ");
		s.append(getId());
		
		s.append(" (");
		int merLength = config.getInt(CONFIG_MER_LENGTH, 1);
		s.append(Mer.fromInt(getId(), merLength));
		s.append(") ");
		
		MRVertex.AdjacencyIterator toIt = createToAdjacencyIterator();
		if (toIt.begin() != NO_VERTEX) {
			s.append("; to: ");
			for (int to = toIt.begin(); !toIt.done(); to = toIt.next()) {
				s.append(to);
				s.append(" ");
			}
		}
		
		MRVertex.AdjacencyIterator fromIt = createFromAdjacencyIterator();
		if (fromIt.begin() != NO_VERTEX) {
			s.append("; from: ");
			for (int from = fromIt.begin(); !fromIt.done(); from = fromIt.next()) {
				s.append(from);
				s.append(" ");
			}
		}
		
		if (merString != null) {
			s.append("; mer ");
			s.append(merString.toDisplayString());
		}
		
		return s.toString();
	}

	//
	
	protected void compressChainInternal(MRVertex other, Configuration config) {
		if (other instanceof MRMerVertex) {
			MRMerVertex otherMer = (MRMerVertex) other;

			int merLength = config.getInt(CONFIG_MER_LENGTH, 1);

			if (merString == null)
				merString = new MerString(getId(), merLength);
			MerString otherMerString = otherMer.merString;
			if (otherMerString == null)
				otherMerString = new MerString(other.getId(), merLength);
				
			// HEY!! Debugging output
			System.out.println("*** " + merString.toDisplayString() + " + " + otherMerString.toDisplayString() + 
					" [" + (merLength - 1) + "] ***");
			
			merString.merge(otherMerString, merLength - 1);
			
			// HEY!! Debugging output
			System.out.println("**** " + merString.toDisplayString() + " ****");
		}
	}
	
	protected byte[] toWritableInternal() {
		if (merString != null)
			return merString.toBytes();
		else
			return null;
	}
	protected void fromWritableInternal(byte[] array, int i, int n) {
		merString = new MerString(array, i, n);
	}
	
	//
	
	private MerString merString;

}
