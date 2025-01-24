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


/**
 * Changelog:
 *
 * Initial:                                       ~8000 ms  — strangely unstable because of IntelliJ profiler, later switched on hyperfine
 * Parallel processing:                           ~1200 ms  — traversing forward byte by byte
 * Change read direction:                         ~1200 ms  — no change but code is much cleaner
 * Read 24 bytes at a time via SWAR token checks: ~800 ms   — Big impact! The real run is 33% faster, even IntelliJ profiler shows worse. Don't trust IntelliJ!
 * Skip redundant bytes:                          ~670 ms   — Another kill! 16% faster after skipping minimum search string amount.
 * Compare by longs:                              ~620 ms   — Instead of byte scanning we scan long and ints to improve speed. 7% faster
 * Refactor main loop, constant loop count:       ~575 ms   - Loop count is always size/8 now, this way compiler can unroll the loop better
 *
 * Testing on JDK 21.0.5-graal JIT compiler (no native)
 *
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

  private static long findNextLineBreak(MemorySegment memory, long offset) {
    long position = offset;
    while (memory.get(ValueLayout.JAVA_BYTE, position) != '\n') { // read until a linebreak
      position++;
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

  private static boolean compare(MemorySegment segment, long offset, TokenizedSearchInput search) {
    long segmentStart = offset;
    int pos = search.size;

    // safely read 8 + 4 = 12 bytes without looping since the search string is minimum 12
    final long long1 = search.firstLong;
    final long long2 = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, segmentStart - 8);
    if (long1 != long2) { // mismatch
      return false;
    }

    final int int1 = search.secondInt;
    final int int2 = segment.get(ValueLayout.JAVA_INT_UNALIGNED, segmentStart - 12);
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
      final byte b2 = segment.get(ValueLayout.JAVA_BYTE, segmentStart);
      if (b1 != b2) { // mismatch
        return false;
      }
      pos--;
      segmentStart--;
    }

    return true;
  }

  static class TokenizedSearchInput {

    // Search String:
    // Ex: ;1;1;1;1;1;1 - min: 12, max: 18 including the first ';'
    // Considering there will be never empty names, min 1 chars: 12 + 1 = 13
    // We use this to skip some extra bytes
    static final int MIN_SEARCH_INPUT = 13;

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

    final MemorySegment segment;
    final long start;
    final long end;
    final TokenizedSearchInput searchInput;

    public RegionWorker(MemorySegment memory, long start, long end, TokenizedSearchInput input) {
      this.segment = memory;
      this.start = start;
      this.end = end;
      this.searchInput = input;
    }

    @Override
    public void run() {
      // System.out.printf("Thread: %s, segment: [%d, %d]%n", Thread.currentThread().getName(), this.start, this.end);
      // final byte lastByte = this.segment.get(ValueLayout.JAVA_BYTE, this.end);
      // System.out.printf("Thread: %s, last byte is line break?: %s%n", Thread.currentThread().getName(), lastByte == '\n');
      // System.out.printf("Thread: %s, last byte: %s%n", Thread.currentThread().getName(), new String(new byte[] { lastByte }));

      long lineBreakPos = this.end;
      long position = this.end; // scan the segment reverse
      final long loopCount = (this.end - this.start) / 8; // 8 bytes at a time
      for (int i = 0; i < loopCount; i++) {
        // there maybe redundant checks here because the linebreak position is not found always
        // instead of adding more branches in hotspot we leave it here since compiler can optimize it well,
        // and it's faster due to instruction level parallelism
        if (compare(this.segment, lineBreakPos, this.searchInput)) { // found a match
          final long start = findPreviousLinebreak(this.segment, lineBreakPos - 1) + 1;
          final long end = lineBreakPos - this.searchInput.size;
          printName(this.segment, start, end);
        }
        position = position - 8; // move pointer 8 bytes to the back
        final long word = this.segment.get(ValueLayout.JAVA_LONG_UNALIGNED, position); // read a word of 8 bytes each time
        final long relativePos = linebreakPos(word); // linebreak position in the word, if not returns 8
        lineBreakPos = position + relativePos;
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

    if (concurrency == 1) { // shortcut for single-thread mode
      new RegionWorker(memory, segmentStart, fileSize - 1, searchInput).start();
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
      new RegionWorker(memory, segmentStart, segmentEnd, searchInput).start(); // start processing
      segmentStart += segmentSize;
    }
  }

}
