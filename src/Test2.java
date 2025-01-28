import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

public class Test2 {

  static class TokenizedSearchInput {

    // TODO docs?
    static final int MAX_INPUT_LENGTH = 18;

    final int size;
    final byte[] bytes;

    // cache the first 12 byte: long + int
    final short firstShort;
    final long firstLong;
    final long secondLong;

    public TokenizedSearchInput(byte[] bytes) {
      this.size = bytes.length;
      this.bytes = new byte[MAX_INPUT_LENGTH]; // max input size
      // copy from backwards, so if the input is smaller than max size first bytes will be zero
      int j = MAX_INPUT_LENGTH - 1; // index 2
      for (int i = bytes.length - 1; i >= 0; i--) {
        this.bytes[j--] = bytes[i];
      }
      final MemorySegment segment = MemorySegment.ofArray(this.bytes);
      this.firstShort = segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, MAX_INPUT_LENGTH - this.size); // first 2 bytes excluding empty bytes
      this.firstLong = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, MAX_INPUT_LENGTH - 8); // from backwards
      this.secondLong = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, MAX_INPUT_LENGTH - this.size + 2); // starting right after first 2 bytes
    }
  }

  /**
   * Keep the bytes from left and clear least significant bytes from right with the given amount of length
   */
  private static long partialLeft(long word, int length) {
    final long mask = (~0L) << (length << 3);
    return word & (mask);
  }

  /**
   * Keep the bytes from right and clear most significant bytes from left with the given amount of length
   */
  private static long partialRight(long word, int length) {
    final long mask = (~0L) << (8 - length << 3);
    return word & (~mask);
  }

  private static void printLong(long word2) {
    System.out.println(new String(ByteBuffer.allocate(8).putLong(word2).array()));
  }

  private static boolean compare2(MemorySegment segment, long offset, TokenizedSearchInput search) {
    // check the first 2 bytes, this includes the first ';' as well
    final short short1 = search.firstShort;
    final short short2 = segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, offset - search.size); // read 2 bytes

    printLong(short1);
    printLong(short2);

    if (short1 != short2) { // mismatch
      return false;
    }

    // safely read 2 longs since max input size is 18, first 2 bytes is read and 16 left
    final long long1 = search.firstLong;
    final long long2 = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset - 8); // first long from backwards

    printLong(long1);
    printLong(long2);

    if (long1 != long2) { // mismatch
      return false;
    }

    final long long3 = search.secondLong;
    long long4 = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset - search.size + 2); // second long from backwards
    printLong(long3);
    printLong(long4);
//    long4 = partialLeft(long4, 16 - search.size); // clear outstanding bytes
//    printLong(long4);

    return long3 == long4; // mismatch or match
  }

  public static void main(String[] args) {

    final String data = "L;4;5;6;9;2;1";
    final MemorySegment segment = MemorySegment.ofArray(data.getBytes());
    final TokenizedSearchInput input = new TokenizedSearchInput(";4;5;6;9;2;1".getBytes());

    final boolean result = compare2(segment, data.length(), input);
    System.out.println(result);
  }

}
