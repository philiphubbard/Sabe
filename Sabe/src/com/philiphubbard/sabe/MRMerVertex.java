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

// A version of MRVertex that stores MerString instances.  Thus, on the
// assumption that the IDs of the vertices encode (k-1)-mers via the
// Mer class, this class can specialize chain compression to keep
// track of the resulting merged MerString.

public class MRMerVertex extends MRVertex {
	
	// Use this property with hadoop.conf.Configuration.setInt() to set the
	// length of the mers (the (k-1) for the (k-1)-mers) associated with
	// each vertex.

	public static final String CONFIG_MER_LENGTH = "CONFIG_MER_LENGTH";
	
	// Construct a vertex with an ID that encodes a (k-1)-mer via the
	// Mer class.  The Configuration is used to get the (k-1) length.
	
	public MRMerVertex(int id, Configuration config) {
		super(id, config);
		merString = new MerString(id, config.getInt(CONFIG_MER_LENGTH, 1));
	}
	
	// Construct a vertex from the hadoop.io.BytesWritable, assumed to
	// have the format produced by MRVertex.toWritable().
	
	public MRMerVertex(BytesWritable writable, Configuration config) {
		super(writable, config);
	}
	
	// Get the MerString associated with this vertex.
	
	public MerString getMerString() {
		return merString;
	}
	
	// Returns true if the values (not the references) of this vertex and
	// the other vertex are equivalent.
	
	public boolean equals(MRMerVertex other) {
		if (!super.equals(other))
			return false;
		return (merString.equals(other.merString));
	}
	
	// Returns a displayable (human readable) string representation of
	// this vertex.
	
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
	
	// Specializes the virtual function from MRVertex, to merge this vertex's
	// MerString with that of the other vertex during chain compression.
	
	@Override
	protected void compressChainInternal(MRVertex other, Configuration config) {
		if (other instanceof MRMerVertex) {
			MRMerVertex otherMer = (MRMerVertex) other;

			int merLength = config.getInt(CONFIG_MER_LENGTH, 1);

			if (merString == null)
				merString = new MerString(getId(), merLength);
			MerString otherMerString = otherMer.merString;
			if (otherMerString == null)
				otherMerString = new MerString(other.getId(), merLength);
			
			merString.merge(otherMerString, merLength - 1);
		}
	}
	
	// Specializes the virtual function from MRVertex to write out this vertex's
	// MerString.
	
	@Override
	protected byte[] toWritableInternal() {
		if (merString != null)
			return merString.toBytes();
		else
			return null;
	}
	
	// Specializes the virtual function from MRVertex to read in this vertex's
	// MerString from the byte array, starting at index i and assuming length n.
	
	@Override
	protected void fromWritableInternal(byte[] array, int i, int n) {
		merString = new MerString(array, i, n);
	}
	
	//
	
	private MerString merString;

}
