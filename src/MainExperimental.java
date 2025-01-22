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

public class MainExperimental {

  private static final Path DATA_FILE = Path.of("pool.csv");

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  private static long hasSemicolon(long word) {
    // semicolon pattern
    final long hasVal = word ^ 0x3B3B3B3B3B3B3B3BL; // hasvalue
    return ((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L); // haszero
  }

  private static int semicolonPos(long hasVal) {
    return Long.numberOfTrailingZeros(hasVal) >>> 3;
  }

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  // returns [0-7] otherwise 8 when no match
  private static int linebreakPos(long word) {
    // // hasvalue
    final long hasVal = word ^ 0xa0a0a0a0a0a0a0aL; // semicolon pattern
    return Long.numberOfTrailingZeros(((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L)) >>> 3; // haszero
  }

  /**
   * Extract bytes of a long from left
   */
  private static long partialLeft(long word, int length) {
    final long mask = (~0L) << (length << 3);
    return word & (mask);
  }

  /**
   * Extract bytes of a long from right
   */
  private static long partialRight(long word, int length) {
    final long mask = (~0L) << (length << 3);
    return word & (~mask);
  }

  /**
   * Scans the given segment to the left
   */
  private static long findPreviousLineBreak(MemorySegment memory, long offset) {
    long position = offset - 1;
    while (memory.get(ValueLayout.JAVA_BYTE, position) != '\n') { // read until a linebreak
      position--;
    }
    return position + 1;
  }

  private static long findNextLineBreak(MemorySegment memory, long offset) {
    long position = offset;
    while (memory.get(ValueLayout.JAVA_BYTE, position) != '\n') { // read until a linebreak
      position++;
    }
    return position;
  }

  private static long findNextSemicolon(MemorySegment memory, long offset) {
    long position = offset;
    while (memory.get(ValueLayout.JAVA_BYTE, position) != ';') { // read until a linebreak
      position++;
    }
    return position;
  }

  /**
   * Read 8 bytes at a time
   */
  private static long findNextSemicolon2(MemorySegment memory, long offset, long limit) {
    long position = offset;
    long hasVal;
    // read long by long until the next long contains a semicolon
    while (position + 24 <= limit) {
      if ((hasVal = hasSemicolon(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position))) != 0) {
        return position + semicolonPos(hasVal);
      } else {
        if ((hasVal = hasSemicolon(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position + 8))) != 0) {
          return position + 8 + semicolonPos(hasVal);
        } else {
          if ((hasVal = hasSemicolon(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position + 16))) != 0) {
            return position + 16 + semicolonPos(hasVal);
          }
        }
      }
      position += 24;
    }
    // read leftovers byte by byte
    while (memory.get(ValueLayout.JAVA_BYTE, position) != ';') { // read until a linebreak
      position++;
    }
    return position;
  }

  private static long findNextSemicolon3(MemorySegment memory, long offset, long limit) {
    long position = offset;
    // read long by long until the next long contains a semicolon
    while (position + 24 <= limit) {
      final long hasVal1 = hasSemicolon(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position));
      final long hasVal2 = hasSemicolon(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position + 8));
      final long hasVal3 = hasSemicolon(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position + 16));

      if (hasVal1 != 0) {
        return position + semicolonPos(hasVal1);
      }
      if (hasVal2 != 0) {
        return position + 8 + semicolonPos(hasVal2);
      }
      if (hasVal3 != 0) {
        return position + 16 + semicolonPos(hasVal3);
      }

      position += 24;
    }
    // read leftovers byte by byte
    while (memory.get(ValueLayout.JAVA_BYTE, position) != ';') { // read until a linebreak
      position++;
    }
    return position;
  }

  private static boolean hasMatch(MemorySegment segment, long offset, MemorySegment search, long searchSize) {
    long match = 0;
    int position = 0;
    while (position < search.byteSize()) {
      // TODO check has space
      final long source = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + position);
      final long check;
      if (position + 8 > searchSize) {
        check = search.get(ValueLayout.JAVA_LONG_UNALIGNED, position);
        match += partialRight(source, (int) (searchSize - position)) ^ check;
      } else {
        check = search.get(ValueLayout.JAVA_LONG_UNALIGNED, position);
        match += source ^ check;
      }
      position += 8;
    }

    return match == 0;
  }

  private static void printName(MemorySegment memory, long index) {
    final long nameStart = findPreviousLineBreak(memory, index);
    final long nameSize = index - nameStart;
    final byte[] bytes = new byte[(int) nameSize];
    memory.asSlice(nameStart, nameSize).asByteBuffer().get(bytes);
    System.out.println(new String(bytes));
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
      // System.out.printf("Thread: %s, segment: [%d, %d]%n", Thread.currentThread().getName(), this.start, this.end);

      // scan the segment
      while (this.start < this.end) {
//        System.out.printf("Thread: %s, start: %d, end: %d%n", Thread.currentThread().getName(), this.start, this.end);

        final long index = findNextSemicolon3(this.segment, this.start, this.end);
//        System.out.printf("Thread: %s, index: %d%n", Thread.currentThread().getName(), index);

        final long offset = index + 1;
        final long size = (offset + searchSize) <= this.end ? searchSize : this.end - offset;
//        final int mismatch = this.segment.asSlice(offset, size).asByteBuffer()
//            .mismatch(this.search);

        // TODO replace with custom equals check, long by long
        final long mismatch = this.segment.asSlice(offset, size).mismatch(search);

        if (mismatch == searchSize) { // we have a match
          printName(this.segment, index);
          this.start = offset + searchSize;
        } else {
          this.start = findNextLineBreak(this.segment, offset) + 1;
        }
      }


    }
  }


  public static void main(String[] args) throws Exception {


    // read input from program args
    final String[] numbers = Arrays.stream(args).limit(6)
        .toArray(String[]::new);
    System.out.println(Arrays.toString(numbers));

    // read the search number into bytes
    final ByteBuffer buffer = ByteBuffer.allocate(24);
    for (String number : numbers) {
      buffer.put(number.getBytes()).put((byte) ';');
    }
    buffer.put(buffer.position() - 1, (byte) '\n'); // replace the last comma with new line
    final int searchSize = buffer.position();
    final MemorySegment search = MemorySegment.ofBuffer(buffer.clear());

    var concurrency = 2 * Runtime.getRuntime().availableProcessors();
    final long fileSize = Files.size(DATA_FILE);
    long regionSize = fileSize / concurrency;

    // handling extreme cases, if the file is too big
    while (regionSize > Integer.MAX_VALUE) {
      concurrency *= 2;
      regionSize /= 2;
    }
    if (fileSize <= 1 << 20) { // small file (1mb), run in 2 threads
      concurrency = 2;
      regionSize = fileSize / 2;
    }

    System.out.println("File size: " + fileSize);
    System.out.println("Region size: " + regionSize);

    long segmentStart = 0;
    final FileChannel channel = (FileChannel) Files.newByteChannel(DATA_FILE, StandardOpenOption.READ);
    final MemorySegment memory = channel.map(MapMode.READ_ONLY, segmentStart, fileSize, Arena.global());

    for (int i = 0; i < concurrency; i++) {
      // calculate boundaries
      final long segmentSize = (segmentStart + regionSize > fileSize) ? fileSize - segmentStart : regionSize;
      // shift position to back until we find a linebreak
      long segmentEnd = findNextLineBreak(memory, segmentStart + segmentSize);
      // calculate last segment
      if (i + 1 == concurrency && segmentEnd < fileSize) {
        segmentEnd = fileSize;
      }
      final RegionWorker region = new RegionWorker(memory, segmentStart, segmentEnd, search, searchSize);
      region.start(); // start processing

      segmentStart += segmentSize;
    }

  }


}
