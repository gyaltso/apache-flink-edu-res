package com.gyaltso.edu.flink.helloworld;

import java.util.Arrays;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Streaming Dataflow (source -> map operator -> sink)
 */
public class HelloWorldFlink {

	public static void main(String[] args) throws Exception {
		// Create a context in which the program is executed
		// Creates an instance of the {@code LocalStreamEnvironment}
		// Source operator
		var env = StreamExecutionEnvironment.getExecutionEnvironment();

		// The Data in the example is finite
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);

		// Source operator
		// Create a Collection Data source
		// The data stream source created will be non-parallel (parallelism set to one)
		var namesDataStream = env.fromCollection(Arrays.asList("Neeraj", "Bhushan", "Kunal", "Yogesh"));

		// Map operator
		// The operator map has parallelism set to 8 (i.e. 8 subtasks)
		var greetDataStream = namesDataStream.map(name -> "Hello, " + name);

		// Sink operator
		// Print the toString() value of each element on the standard output
		// If the parallelism is grater than 1, the output is prepended with the
		// identifier of the task which produced the output.
		greetDataStream.print();

		env.execute("Hello World Flink");
	}

}
