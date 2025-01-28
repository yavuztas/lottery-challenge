import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;


/**
 * Changelog:
 *
 * Initial:                                                               ~8000 ms  — strangely unstable because of IntelliJ profiler, later switched on hyperfine
 * Parallel processing:                                                   ~1200 ms  — traversing forward byte by byte
 * Change read direction:                                                 ~1200 ms  — no change but code is much cleaner
 * Read 24 bytes at a time via SWAR token checks:                         ~800 ms   — Big impact! The real run is 33% faster, even IntelliJ profiler shows worse. Don't trust IntelliJ!
 * Skip redundant bytes:                                                  ~670 ms   — Another kill! 16% faster after skipping minimum search string amount.
 * Compare by longs:                                                      ~620 ms   — Instead of byte scanning we scan long and ints to improve speed. 7% faster
 * Refactor main loop, constant loop count:                               ~575 ms   - Loop count is always size/8 now, this way compiler can unroll the loop better
 * Refine code to match numbers independently of their orders             ~2300 ms  - Back to seconds again, but matching without order is expensive
 * Implement a lookup table, calculate possible permitations at startup:
 *  - Use lookup table to compare in o(1)                                 ~1400ms   - Still hoping to get under 1 seconds, improving...
 * Custom Set implementation to eleminate extra checks                    ~950ms    - Eleminating extra checks helped a lot, back under 1 second!
 * Increase bucket size, adjust JVM options                               ~870ms    - increasing bucket size from 16K to 256K eleminates collisions (almost) and gives us ~80ms bonus
 *
 *
 * Testing on JDK 21.0.5-graal JIT compiler (no native), limiting to 8 threads.
 * Big thanks to Mike, for bringing this challenge.
 *
 * Follow me at: github.com/yavuztas
 */
public class Main {

  private static final Path DATA_FILE = Path.of("pool.csv");

  private static void printName(MemorySegment segment, long start, long end) {
    final ByteBuffer buffer = segment.asSlice(start, end - start).asByteBuffer();
    final byte[] bytes = new byte[(int) (end - start)];
    buffer.get(bytes);
    System.out.println(new String(bytes, UTF_8));
  }

  private static long findPreviousLinebreak(MemorySegment segment, long offset) {
    long position = offset;
    while (segment.get(ValueLayout.JAVA_BYTE, position) != '\n') { // read until a linebreak
      position--;
      if (position == 0) // no newline found
        return -1;
    }
    return position;
  }

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  // returns [0-7] otherwise 8 when no match
  private static int linebreakPos(long word) {
    // // hasvalue
    final long hasVal = word ^ 0xa0a0a0a0a0a0a0aL; // semicolon pattern
    return Long.numberOfTrailingZeros(((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L)) >>> 3; // haszero
  }

  static final InputSet inputSet = new InputSet();

  private static boolean compare(MemorySegment segment, long offset, TokenizedSearchInput searchInput, int inputLength) {
    // get the first 2 bytes, this includes the first ';' as well
    final short firstShort = segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, offset - inputLength); // read 2 bytes
    final long firstLong = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset - 8); // first long from backwards
    final long secondLong = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset - inputLength + 2); // second long from backwards

    // reuse search input and check it from lookup cache
    return inputSet.contains(searchInput.reset(firstShort, firstLong, secondLong));
  }

  // Custome Set on purpose, less possible checks
  static final class InputSet {

    // Bigger bucket size less collisions, but you have to find a sweet spot otherwise it becomes slower.
    private static final int SIZE = 1 << 18; // 256kb
    private static final int BITMASK = SIZE - 1;
    private final TokenizedSearchInput[] values = new TokenizedSearchInput[SIZE];

    private static int hashBucket(int hash) {
      hash = hash ^ (hash >>> 16); // naive bit spreading but surprisingly decreases collision :)
      return hash & BITMASK; // fast modulo, to find bucket
    }

    void add(TokenizedSearchInput input) {
      final int bucket = hashBucket(input.hashCode());
      TokenizedSearchInput existing = this.values[bucket];
      if (existing == null) {
        this.values[bucket] = input;
        return;
      }
      // collision, linear probing to find a slot
      // NOTE: here we don't do equals check on purpose since we don't add the same input twice
      // If we do same input will be added twice but we don't, never :)
      while (existing.next != null) {
        existing = existing.next;
      }
      existing.next = input;
    }

    boolean contains(TokenizedSearchInput input) {
      final int bucket = hashBucket(input.hashCode());
      TokenizedSearchInput existing = this.values[bucket];
      if (existing == null) {
        return false;
      }

      boolean contains = false;
      while (!contains && existing != null) {
        contains = existing.equals(input);
        existing = existing.next;
      }
      return contains;
    }
  }

  static class TokenizedSearchInput {

    // linked list for collisions
    TokenizedSearchInput next;

    // split bytes into 2 + 8 + 8 (short, long, long)
    short firstShort;
    long firstLong;
    long secondLong;

    public TokenizedSearchInput() {
      this.firstShort = 0;
      this.firstLong = 0;
      this.secondLong = 0;
    }

    public TokenizedSearchInput(byte[] bytes) {
      final MemorySegment segment = MemorySegment.ofArray(bytes);
      this.firstShort = segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, 0); // first 2 bytes excluding empty bytes
      this.firstLong = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, bytes.length - 8); // from backwards
      this.secondLong = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 2); // right after first 2 bytes
    }

    TokenizedSearchInput reset(short firstShort, long firstLong, long secondLong) {
      this.firstShort = firstShort;
      this.firstLong = firstLong;
      this.secondLong = secondLong;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      final TokenizedSearchInput that = (TokenizedSearchInput) o;
      return this.firstShort == that.firstShort && this.firstLong == that.firstLong && this.secondLong == that.secondLong;
    }

    @Override
    public int hashCode() {
      int result = 1;
      // mersenne prime 31
      result = ((result << 5) - result) + this.firstShort;
      result = ((result << 5) - result) + Long.hashCode(this.firstLong);
      result = ((result << 5) - result) + Long.hashCode(this.secondLong);
      return result;
    }
  }

  static class RegionWorker extends Thread {

    final MemorySegment segment;
    final long start;
    final long end;

    final TokenizedSearchInput searchInput = new TokenizedSearchInput();
    final TokenizedSearchInput searchInput2 = new TokenizedSearchInput();
    final int inputLength;

    public RegionWorker(MemorySegment memory, long start, long end, int inputLength) {
      this.segment = memory;
      this.start = start;
      this.end = end;
      this.inputLength = inputLength;
    }

    @Override
    public void run() {
      // System.out.printf("Thread: %s, segment: [%d, %d]%n", Thread.currentThread().getName(), this.start, this.end);
      // final byte lastByte = this.segment.get(ValueLayout.JAVA_BYTE, this.end);
      // System.out.printf("Thread: %s, last byte is line break?: %s%n", Thread.currentThread().getName(), lastByte == '\n');
      // System.out.printf("Thread: %s, last byte: %s%n", Thread.currentThread().getName(), new String(new byte[] { lastByte }));
      long word;
      long relativePos = -1;
      long lineBreakPos = this.end;
      long position = this.end; // scan the segment reverse
      final long loopCount = (this.end - this.start) / 8; // 8 bytes at a time
      for (int i = 0; i < loopCount; i++) {
        // there maybe redundant checks here because the linebreak position is not always correct
        // when no linebreak match relativePos will be 8. In such cases, line break positions will be duplicated.
        // Therefore, this can produce duplicated winners.
        // However, instead of adding more branches in hotspot we leave it here since compiler can optimize it much better,
        // and it's faster due to instruction level parallelism
        if (relativePos !=8 && compare(this.segment, lineBreakPos, this.searchInput, this.inputLength)) { // found a match
          final long start = findPreviousLinebreak(this.segment, lineBreakPos - 1) + 1;
          final long end = lineBreakPos - this.inputLength;
          printName(this.segment, start, end);
        }
        
        word = this.segment.get(ValueLayout.JAVA_LONG_UNALIGNED, position - 8); // read a word of 8 bytes each time
        relativePos = linebreakPos(word); // linebreak position in the word, if not returns 8
        lineBreakPos = position - 8 + relativePos;

        position -= 8; // move pointer 8 bytes to the back
      }
    }
  }

  private static void swap(String[] elements, int a, int b) {
    final String tmp = elements[a];
    elements[a] = elements[b];
    elements[b] = tmp;
  }

  private static void cacheSearchInput(String[] input) {
    final byte[] bytes = (";" + String.join(";", input)).getBytes(); // prepend ';' to match correctly
    inputSet.add(new TokenizedSearchInput(bytes));
  }

  // copied from: https://www.baeldung.com/java-array-permutations
  static void generatePermutations(String[] input) {
    cacheSearchInput(input);
    final int n = input.length;
    final int[] indexes = new int[n];
    int i = 0;
    while (i < n) {
      if (indexes[i] < i) {
        swap(input, i % 2 == 0 ?  0: indexes[i], i);
        cacheSearchInput(input);
        indexes[i]++;
        i = 0;
      } else {
        indexes[i] = 0;
        i++;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Input: " + Arrays.toString(args));
    // build lookup table, for 6 numbers it's 6! = 720
    // Since we do it only once at startup it's no impact on performance
    generatePermutations(args);

    var concurrency = 8; //2 * Runtime.getRuntime().availableProcessors();
    final long fileSize = Files.size(DATA_FILE);
    long regionSize = fileSize / concurrency;

    if (fileSize <= 1 << 20) { // small file (under 1mb), run in single-thread mode
      concurrency = 1;
      regionSize = fileSize;
    }

    System.out.println("Concurrency: " + concurrency);
    System.out.println("File size: " + fileSize);
    System.out.println("Region size: " + regionSize);

    long segmentStart = 0;
    final FileChannel channel = (FileChannel) Files.newByteChannel(DATA_FILE, StandardOpenOption.READ);
    final MemorySegment memory = channel.map(MapMode.READ_ONLY, segmentStart, fileSize, Arena.global());

    final int inputLength = (";" + String.join(";", args)).getBytes().length;
    if (concurrency == 1) { // shortcut for single-thread mode
      new RegionWorker(memory, segmentStart, fileSize, inputLength).start();
      return;
    }

    // calculate boundaries for regions
    for (int i = 0; i < concurrency - 1; i++) {
      new RegionWorker(memory, segmentStart, segmentStart + regionSize, inputLength).start(); // start processing
      segmentStart += regionSize;
    }
    new RegionWorker(memory, segmentStart, fileSize, inputLength).start(); // last piece
  }

}
