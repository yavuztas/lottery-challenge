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
 * Initial:                                       ~8000 ms  — strangely unstable because of IntelliJ profiler, later switched on hyperfine
 * Parallel processing:                           ~1200 ms  — traversing forward byte by byte
 * Change read direction:                         ~1200 ms  — no change but code is much cleaner
 * Read 24 bytes at a time via SWAR token checks: ~800 ms   — Big impact! The real run is 33% faster, even IntelliJ profiler shows worse. Don't trust IntelliJ!
 * Skip redundant bytes:                          ~670 ms — Another kill! 16% faster after skipping minimum search string amount. 
 * ?
 * 
 * Testing on JDK 21.0.5-graal JIT compiler (no native)
 *
 * Big thanks to Mike, for bringing this challenge.
 *
 * Follow me at: github.com/yavuztas
 */
public class Main {

  private static final Path DATA_FILE = Path.of("pool.csv");

  // Search String: 
  // Ex: ;1;1;1;1;1;1 - min: 12, max: 18 including the first ';'
  // Considering there will be never empty names, min 1 chars: 12 + 1 = 13
  // We use this to skip some extra bytes
  private static final int MIN_SEARCH_INPUT = 13;  

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

  private static long findPreviousLinebreak(MemorySegment memory, long offset, long limit) {
    long position = offset;
    // read long by long until the next long contains a semicolon
    while (position - 16 > limit) {
      final long hasVal1 = linebreakPos(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position - 8));
      final long hasVal2 = linebreakPos(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position - 16));
      // reading more doesn't improve

      if (hasVal1 != 8) {
        return position - 8 + hasVal1;
      }
      if (hasVal2 != 8) {
        return position - 16 + hasVal2;
      }

      position -= 16; // 16 bytes at a time
    }
    // read leftovers byte by byte
    return findPreviousLinebreak(memory, position);
  }


  private static boolean compare(MemorySegment segment, long offset, MemorySegment search) {
    long segmentStart = offset;
    for (long i = search.byteSize() - 1; i >= 0; i--) {
      final byte b1 = search.get(ValueLayout.JAVA_BYTE, i);
      final byte b2 = segment.get(ValueLayout.JAVA_BYTE, segmentStart);
      if (b1 != b2) { // mismatch
        return false;
      }
      segmentStart--;
    }

    return true;
  }

  static class RegionWorker extends Thread {

    final MemorySegment segment;
    long start;
    long end;
    final MemorySegment search;
    int searchSize;


    public RegionWorker(MemorySegment memory, long start, long end, MemorySegment search, int searchSize) {
      this.segment = memory;
      this.search = search;
      this.start = start;
      this.end = end;
      this.searchSize = searchSize;
    }

    @Override
    public void run() {
//      System.out.printf("Thread: %s, segment: [%d, %d]%n", Thread.currentThread().getName(), this.start, this.end);
//      final byte lastByte = this.segment.get(ValueLayout.JAVA_BYTE, this.end);
//      System.out.printf("Thread: %s, last byte is line break?: %s%n", Thread.currentThread().getName(), lastByte == '\n');
//      System.out.printf("Thread: %s, last byte: %s%n", Thread.currentThread().getName(), new String(new byte[] { lastByte }));

      // scan the segment reverse
      long position = this.end - 1; // skip the linebreak at the end
      while (position > this.start) {
        if (compare(this.segment, position, this.search)) { // found a match
          final long start = findPreviousLinebreak(this.segment, position) + 1;
          final long end = position - this.searchSize + 1;
          printName(this.segment, start, end);
        }
        position = findPreviousLinebreak(this.segment, position - MIN_SEARCH_INPUT, this.start) - 1;
      }
    }
  }

  private static long findNextLineBreak(MemorySegment memory, long offset) {
    long position = offset;
    while (memory.get(ValueLayout.JAVA_BYTE, position) != '\n') { // read until a linebreak
      position++;
    }
    return position;
  }

  public static void main(String[] args) throws Exception {
    // read input from program args
    final String[] numbers = Arrays.stream(args).limit(6).toArray(String[]::new);
    System.out.println("Input:" + Arrays.toString(numbers));

    // read search numbers into bytes to access via MemorySegment
    final ByteBuffer buffer = ByteBuffer.wrap((";" + String.join(";", args)).getBytes());// prepend ';' match correctly
    final MemorySegment search = MemorySegment.ofBuffer(buffer);

    var concurrency = 8; //* Runtime.getRuntime().availableProcessors();
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

    if (concurrency == 1) { // shortcut for single-thread mode
      new RegionWorker(memory, segmentStart, fileSize - 1, search, (int) search.byteSize()).start();
      return;
    }

    // calculate boundaries for regions
    for (int i = 0; i < concurrency; i++) {
      final long segmentSize = (segmentStart + regionSize > fileSize) ? fileSize - segmentStart : regionSize;
      // shift position to the next until we find a linebreak
      long segmentEnd = findNextLineBreak(memory, segmentStart + segmentSize); // last byte of a region is always a '\n'
      // calculate last segment
      if (i + 1 == concurrency && segmentEnd < fileSize) {
        segmentEnd = fileSize - 1;
      }
      new RegionWorker(memory, segmentStart, segmentEnd, search, (int) search.byteSize()).start(); // start processing
      segmentStart += segmentSize;
    }
  }

}
