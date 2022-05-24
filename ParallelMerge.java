import java.util.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.IntStream;

public class ParallelMerge extends Thread {
	//N list size
	//K number of threads 
	static int K, N;
	//gloable variable Array
	static int[] A;
	//temprory 2d matrix to save the subarrays after sorting
	static int[][] output;
	static int[][] indec;
	//start and end index for the sub sets
	int sr;
	int end;

	ParallelMerge(int[] A) {
		this.A = A;
	}

	ParallelMerge() {
		this.A = A;
	}

	ParallelMerge(int sr, int end) {
		this.sr = sr;
		this.end = end;
	}

	public void run() {

		Arrays.sort(this.A, this.sr, this.end);		 

	}
	

	// To create the array of random int of size N
	public static int[] start(int N) {
		Random rand = new Random(System.currentTimeMillis());
		int[] s = new int[N];
		for (int i = 0; i < N; i++) {
			s[i] = rand.nextInt(10*N);
			System.out.print(s[i] + " ");
		}
		return s;
	}

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		// Take line argument as input for K , N values 
		N=Integer.parseInt(args[0]);
		K= Integer.parseInt(args[1]);
		
		//To calculte the number of cores avaliabe 
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("The Available Cores are: " + cores);
		//declearing the temprory matrix size 
		 output = new int[K][];
		 indec= new int[K][2];
		 //idicates whiter the sub array will all have equal sizes
		int fsbl = N % K;// 1
		System.out.println("Original Array");
		//create the random array
		A = start(N);
		System.out.println("");
		System.out.println("size:" + N + " Threads:" + K);
		//From here start counting the time
		Long inputstat = System.nanoTime();
		// Thread object decleration 
		ParallelMerge r1 = new ParallelMerge();

		if (fsbl != 0) {
			// if the sub arrays does not have the same size
			//sblsize the size of array with big prttiions 
			int sblsize = (int) Math.ceil((float) N / (float) K);
			// restbbl the number of sub arrays with size of  sblsize -1
			int restbbl = K - fsbl;// 2
			
			int resblsize = sblsize - 1;// 3
			int en = 0;
			int s = 0;
			for (int i = 0; i < fsbl; i++) {
				for (int j = 0; j < sblsize; j++) {
					en++;
				}
				//creating thread for every sub array
				indec[i][0]= s;
				indec[i][1]= en-1;
				r1 = new ParallelMerge(s, en);
				r1.start();
				System.out.println("Thread ID: " + r1.getId());
				System.out.println("From index:" + s + " TO Index: " + (en - 1));
				
				s = s + sblsize;
			}
			//for the sub arrays with size n-1
			int s1 = s;
			for (int i = (fsbl); i < (fsbl + restbbl); i++) {
				for (int j = 0; j < resblsize; j++) {
					en++;
				}
				indec[i][0]= s1;
				indec[i][1]= en-1;
				 r1 = new ParallelMerge(s1, en);
				 r1.start();
				System.out.println("Thread ID: " + r1.getId());
				System.out.println("From index:" + s1 + " TO Index: " + (en - 1));
				s1 = s1 + resblsize;

			}

			for (int i =0;i<K;i++) {
				try {
					r1.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			int sblsize = (int) Math.ceil((float) N / (float) K);// 5
			int en = 0;
			int s = 0;
//k=2 0 1
			for (int i = 0; i < K; i++) {
				// 0 -5
				for (int j = 0; j < sblsize; j++) {

					en++;
				}
				indec[i][0]= s;
				indec[i][1]= en-1;
				r1 = new ParallelMerge(s, en);
				r1.start();
				System.out.println("Thread ID: " + r1.getId());
				System.out.println("From index:" + s + " TO Index: " + (en - 1));
				s = s + sblsize;

			}
			for (int i =0;i<K;i++ ) {
				try {
					r1.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		
		if (fsbl != 0) {
			int sblsize = (int) Math.ceil((float) N / (float) K);// 4
			int restbbl = K - fsbl;// 2
			int resblsize = sblsize - 1;// 3
			int en = 0;
			int s = 0;
			for (int i = 0; i < fsbl; i++) {
				for (int j = 0; j < sblsize; j++) {
					en++;
				}
				output[i] = Arrays.copyOfRange(A, s, en);
				s = s + sblsize;

			}
			int s1 = s;
			for (int i = (fsbl); i < (fsbl + restbbl); i++) {
				for (int j = 0; j < resblsize; j++) {
					en++;
				}
				output[i] = Arrays.copyOfRange(A, s1, en);
			s1 = s1 + resblsize;}
}
			else {
			int sblsize = (int) Math.ceil((float) N / (float) K);// 5
			int en = 0;
			int s = 0;
			for (int i = 0; i < K; i++) {
				for (int j = 0; j < sblsize; j++) {
					en++;
				}
				output[i] = Arrays.copyOfRange(A, s, en);
				s=s+sblsize;
			}
			}
		 
		int[] qw = mergeKSortedArrays();
		
		Long inputEnd = System.nanoTime();
		System.out.println();

		
		System.out.println("New Array");
		 
		
		  for (int i = 0; i < qw.length; i++) {
		  
		  System.out.print(qw[i] + " ");}
		  
		  System.out.println();
		 
		 

		System.out.println(" Took-------------->" + (inputEnd - inputstat) / 1000000 + "ms");

	}

	static int[] mergeKSortedArrays() {
		ParallelMerge s= new ParallelMerge();
		int[][] arrays = s.output ;
		int [][] ind=s.indec;
		PriorityQueue<HeapNode> AueueForSubA = new PriorityQueue<>();
		int resultLen = 0;

		// Add first element of each array to the queue
		int q=0;
		for (int[] arr : arrays) {
			System.out.println("Now megering from A subarrayy from indx " + ind[q][0]+" To " +ind[q][1]);

			AueueForSubA.add(new HeapNode(arr, 0));
			resultLen += arr.length;
			q++;
		}

		int[] result = new int[resultLen];
		int i = 0;

		while (!AueueForSubA.isEmpty()) {
			// Extract min element from the queue
			HeapNode node = AueueForSubA.poll();
			result[i] = node.arr[node.index];
			i++;

			// Add to queue next element from the array that contains the current visited
			// node
			if (node.hasNext()) {
				AueueForSubA.add(new HeapNode(node.arr, node.index + 1));
			}
		}

		return result;
	}

	// Node class implementation
	static class HeapNode implements Comparable<HeapNode> {
		int[] arr;
		int index;

		public HeapNode(int[] arr, int index) {
			this.arr = arr;
			this.index = index;
		}

		public boolean hasNext() {
			return this.index < this.arr.length - 1;
		}

		@Override
		public int compareTo(HeapNode o) {
			return arr[index] - o.arr[o.index];
		}
	}

}
