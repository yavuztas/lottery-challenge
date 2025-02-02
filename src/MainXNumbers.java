import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;


/**
 * Changelog:
 *
 * Initial with number parsing              :     ~1500 ms
 * Eleminate copies and buffer print stream :     ~1200 ms
 *
 *
 * Testing on JDK 21.0.5-graal JIT compiler (no native), limiting to 8 threads.
 * Big thanks to Mike, for bringing this challenge.
 *
 * Follow me at: github.com/yavuztas
 */
public class MainXNumbers {

  private static final Path DATA_FILE = Path.of("pool.csv");

  private static final int MAX_NAME_LENGTH = 1024; // maxiumum possible name length to print winners in a more performant way
  private static final int MATCH_X_NUMBERS = 5; // how many numbers to match for a winner

  // Custome print stream to buffer all output and flush once in the end. This is faster when we have a lot of winners
  private static final int PRINT_STREAM_BUFFER_SIZE = 1 << 17; // 128k - enough for apprx. 5k winners
  private static final PrintStream OUT = new PrintStream(new BufferedOutputStream(new FileOutputStream(FileDescriptor.out), PRINT_STREAM_BUFFER_SIZE), false, UTF_8);

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  // returns [0-7] otherwise 8 when no match
  private static int linebreakPos(long word) {
    // hasvalue
    final long hasVal = word ^ 0xa0a0a0a0a0a0a0aL; // semicolon pattern
    return Long.numberOfTrailingZeros(((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L)) >>> 3; // haszero
  }

  private static int compareNumbers(MemorySegment segment, long offset, TokenizedSearchInput searchInput) {
    int score = 0;
    int number;
    int limit = 0;
    long pos = offset - 1;
    while (limit < 6) { // only search for 6 numbers
      final byte b1 = segment.get(ValueLayout.JAVA_BYTE, pos);
      final byte b2 = segment.get(ValueLayout.JAVA_BYTE, pos - 1);
      number = b1 - 0x30;
      if (b2 != ';') {
        pos--; // skip next ';'
        number += (b2 - 0x30) * 10;
      }
      pos -= 2;
      score += searchInput.numbers[number];
      limit++;
    }
    return score;
  }

  static class TokenizedSearchInput {

    final int length;
//    final int[] numbers = new int[1 << 16]; // 64K, can be addressed with unsigned short

    // store numbers in array index
    final int[] numbers = new int[50]; // can store [0-49]

    public TokenizedSearchInput(String[] args) {
      // we prepend extra 2 bytes to avoid out of bounds errors
      final byte[] bytes = ("x;" + String.join(";", args)).getBytes();
      this.length = bytes.length - 2; // exclude extra 2

      /*
      final MemorySegment segment = MemorySegment.ofBuffer(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
      int limit = 0;
      long pos = bytes.length - 1;
      while (limit < 6) { // only search for 6 numbers
        final short word = Short.reverseBytes(segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, pos - 1));
        final int semiColonPos = semiColonPos(word);
        pos -= semiColonPos + 1;
        this.numbers[word] = 1;
        limit++;
      }*/

      int number;
      int pos = bytes.length - 1;
      while (pos > 0) { // parse numbers as int starting from backwards
        final byte b1 = bytes[pos];
        final byte b2 = bytes[pos-1];
        number = b1 - 0x30;
        if (b2 != ';') {
          pos--; // skip next ';'
          number += (b2 - 0x30) * 10;
        }
        pos -= 2;
        this.numbers[number] = 1;
      }
    }
  }

  static class RegionWorker extends Thread {

    final MemorySegment segment;
    final long start;
    final long end;

    final TokenizedSearchInput searchInput;
    final byte[] printBuffer = new byte[MAX_NAME_LENGTH]; // re-use buffer to eleminate byte-array copy

    public RegionWorker(MemorySegment memory, long start, long end, TokenizedSearchInput searchInput) {
      this.segment = memory;
      this.start = start;
      this.end = end;
      this.searchInput = searchInput;
    }

    private void printName(long start, long end) {
      final int length = (int) (end - start);
      MemorySegment.copy(this.segment, JAVA_BYTE, start, this.printBuffer, 0, length);
      this.printBuffer[length] = '\n'; // append new line
      OUT.write(this.printBuffer, 0, length + 1);
    }

    @Override
    public void run() {
      // System.out.printf("Thread: %s, segment: [%d, %d]%n", Thread.currentThread().getName(), this.start, this.end);
      // final byte lastByte = this.segment.get(ValueLayout.JAVA_BYTE, this.end);
      // System.out.printf("Thread: %s, last byte is line break?: %s%n", Thread.currentThread().getName(), lastByte == '\n');
      // System.out.printf("Thread: %s, last byte: %s%n", Thread.currentThread().getName(), new String(new byte[] { lastByte }));
      long word;
      long relativePos = 8;
      long lineBreakPos = this.end;
      long position = this.end; // scan the segment reverse
      final long loopCount = (this.end - this.start) / 8; // 8 bytes at a time
      for (int i = 0; i < loopCount; i++) {
        // there maybe redundant checks here because the linebreak position is not always correct
        // when no linebreak match relativePos will be 8. In such cases, line break positions will be duplicated.
        // Therefore, this can produce duplicated winners.
        // However, instead of adding more branches in hotspot we leave it here since compiler can optimize it much better,
        // and it's faster due to instruction level parallelism
        if (relativePos !=8 && compareNumbers(this.segment, lineBreakPos, this.searchInput) == MATCH_X_NUMBERS) { // found a match
          // scan back to find name bounderies
          byte b;
          int semicolonCount = 0;
          long start = lineBreakPos - 1;
          long end = lineBreakPos - this.searchInput.length;
          while ((b = this.segment.get(ValueLayout.JAVA_BYTE, start)) != '\n') { // read until a linebreak
            start--;
            if (b == ';') semicolonCount++;
            if (semicolonCount == 6) {
              semicolonCount = 0;
              end = start; // found name end
            }
            if (start == 0) {
              start = -1; // no newline found
              break;
            }
          }
          printName(start + 1, end + 1);
        }

        word = this.segment.get(ValueLayout.JAVA_LONG_UNALIGNED, position - 8); // read a word of 8 bytes each time
        relativePos = linebreakPos(word); // linebreak position in the word, if not returns 8
        lineBreakPos = position - 8 + relativePos;

        position -= 8; // move pointer 8 bytes to the back
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Input: " + Arrays.toString(args));

    // build input search string
    final TokenizedSearchInput searchInput = new TokenizedSearchInput(args);

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

    // print all buffered output after JVM exits
    Runtime.getRuntime().addShutdownHook(new Thread(OUT::flush));

    long segmentStart = 0;
    final FileChannel channel = (FileChannel) Files.newByteChannel(DATA_FILE, StandardOpenOption.READ);
    final MemorySegment memory = channel.map(MapMode.READ_ONLY, segmentStart, fileSize, Arena.global());

    if (concurrency == 1) { // shortcut for single-thread mode
      new RegionWorker(memory, segmentStart, fileSize, searchInput).start();
      return;
    }

    // calculate boundaries for regions
    for (int i = 0; i < concurrency - 1; i++) {
      new RegionWorker(memory, segmentStart, segmentStart + regionSize, searchInput).start(); // start processing
      segmentStart += regionSize;
    }
    new RegionWorker(memory, segmentStart, fileSize, searchInput).start(); // last piece
  }

}
