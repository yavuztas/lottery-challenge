import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import sun.misc.Unsafe;


/**
 * Unsafe version of the original solution.
 * Just for fun I'd like to push the limits.
 * Do not use in production :)
 *
 * Testing on JDK 21.0.5-graal, Mac M4 ARM 10 Core CPU: 768K L1i, 512K L1d, 64MB L2, 8MB System Level Cache
 * 
 * JIT compiler (no native), 8 cores    : 538 ms
 * JIT compiler (no native), 10 cores   : 428 ms
 * Graalvm Native image, 8 cores        : 390 ms
 * Graalvm Native image, 10 cores       : 347 ms
 *
 * Big thanks to Mike, for bringing this challenge.
 *
 * Follow me at: github.com/yavuztas
 */
public class MainUnsafe {

  private static final Path DATA_FILE = Path.of("pool.csv");

  private static final Unsafe UNSAFE = unsafe();

  private static Unsafe unsafe() {
    try {
      final Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      return (Unsafe) f.get(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void printName(long address, long start, long end) {
    final byte[] bytes = new byte[(int) (end - start)];
    UNSAFE.copyMemory(null, address + start, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, end - start);
    System.out.println(new String(bytes, StandardCharsets.UTF_8));
  }

  private static long findPreviousLinebreak(long memoryAddress, long offset) {
    long position = offset;
    while (UNSAFE.getByte(memoryAddress + position) != '\n') { // read until a linebreak
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

  private static boolean compare(long offset, TokenizedSearchInput search) {
    long segmentStart = offset;
    int pos = search.size;

    // safely read 8 + 4 = 12 bytes without looping since the search string is minimum 12
    final long long1 = search.firstLong;
    final long long2 = UNSAFE.getLong(segmentStart - 8);
    if (long1 != long2) { // mismatch
      return false;
    }

    final int int1 = search.secondInt;
    final int int2 = UNSAFE.getInt(segmentStart - 12);
    if (int1 != int2) { // mismatch
      return false;
    }
    // udpate positions
    segmentStart -= 13;
    pos -= 13;
    // scan the rest byte by byte
    while (pos >= 0) {
      final byte b1 = search.bytes[pos];
      // TODO boundery check for segment?
      final byte b2 = UNSAFE.getByte(segmentStart);
      if (b1 != b2) { // mismatch
        return false;
      }
      pos--;
      segmentStart--;
    }

    return true;
  }

  static class TokenizedSearchInput {

    final int size;
    final byte[] bytes;

    // cache the first 12 byte: long + int
    final long firstLong;
    final int secondInt;

    public TokenizedSearchInput(byte[] bytes) {
      final MemorySegment segment = MemorySegment.ofArray(bytes);
      this.size = bytes.length;
      this.bytes = bytes;
      this.firstLong = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, this.size - 8); // from backwards
      this.secondInt = segment.get(ValueLayout.JAVA_INT_UNALIGNED, this.size - 12); // from backwards
    }
  }

  static class RegionWorker extends Thread {

    final long address;
    final long start;
    final long end;
    final TokenizedSearchInput searchInput;

    public RegionWorker(long address, long start, long end, TokenizedSearchInput input) {
      this.address = address;
      this.start = start;
      this.end = end;
      this.searchInput = input;
    }

    @Override
    public void run() {
      // System.out.printf("Thread: %s, memory address: %s, segment: [%d, %d]%n", Thread.currentThread().getName(), this.address, this.start, this.end);
      // final byte lastByte = this.segment.get(ValueLayout.JAVA_BYTE, this.end);
      // System.out.printf("Thread: %s, last byte is line break?: %s%n", Thread.currentThread().getName(), lastByte == '\n');
      // System.out.printf("Thread: %s, last byte: %s%n", Thread.currentThread().getName(), new String(new byte[] { lastByte }));

      long position = this.end; // scan the segment reverse
      final long loopCount = (this.end - this.start) / 8; // 8 bytes at a time
      for (int i = 0; i < loopCount; i++) {
        position -= 8; // move pointer 8 bytes to the back
        final long word = UNSAFE.getLong(this.address + position); // read a word of 8 bytes each time
        final long relativePos = linebreakPos(word); // linebreak position in the word, if not returns 8
        final long lineBreakPos = position + relativePos;
        // there maybe redundant checks here because the linebreak position is not found always
        // instead of adding more branches in hotspot we leave it here since compiler can optimize it well,
        // and it's faster due to instruction level parallelism
        if (compare(this.address + lineBreakPos, this.searchInput)) { // found a match
          final long start = findPreviousLinebreak(this.address, lineBreakPos - 1) + 1;
          final long end = lineBreakPos - this.searchInput.size;
          printName(this.address, start, end);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // read input numbers into bytes to access via MemorySegment
    final String numbers = String.join(";", args);
    System.out.println("Input: " + numbers);

    final byte[] bytes = (";" + numbers).getBytes(); // prepend ';' match correctly
    final TokenizedSearchInput searchInput = new TokenizedSearchInput(bytes);

    var concurrency = 2 * Runtime.getRuntime().availableProcessors();
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
    final long memoryAddress = memory.address();

    if (concurrency == 1) { // shortcut for single-thread mode
      new RegionWorker(memoryAddress, segmentStart, fileSize, searchInput).start();
      return;
    }

    // calculate boundaries for regions
    for (int i = 0; i < concurrency - 1; i++) {
      new RegionWorker(memoryAddress, segmentStart, segmentStart + regionSize, searchInput).start(); // start processing
      segmentStart += regionSize;
    }
    new RegionWorker(memoryAddress, segmentStart, fileSize, searchInput).start(); // last piece
  }

}
