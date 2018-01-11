package pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class SortPair implements WritableComparable{
	private Text word;
	private double average;

	public SortPair() {
		word = new Text();
		average = 0.0;
	}

	public SortPair(Text word, double average) {
		//TODO: constructor
		this.word = word;
		this.average = average;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		out.writeDouble(average);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.word.readFields(in);
		this.average = in.readDouble();
	}

	public Text getWord() {
		return word;
	}

	public double getAverage() {
		return average;
	}

	@Override
	public int compareTo(Object o) {

		double thisAverage = this.getAverage();
		double thatAverage = ((SortPair)o).getAverage();

		Text thisWord = this.getWord();
		Text thatWord = ((SortPair)o).getWord();
		
		if (thisAverage > thatAverage) return -1;
		else if (thisAverage < thatAverage) return 1;
		else return thisWord.toString().compareTo(thatWord.toString());
		// Compare between two objects
		// First order by average, and then sort them lexicographically in ascending order
	}
} 
