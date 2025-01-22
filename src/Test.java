import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Test {

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  private static long hasSemicolon(long word) {
    // semicolon pattern
    final long hasVal = word ^ 0x3B3B3B3B3B3B3B3BL; // hasvalue
    return ((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L); // haszero
  }

  private static int semicolonPos(long word) {
    // semicolon pattern
    final long hasVal = word ^ 0x3B3B3B3B3B3B3B3BL; // hasvalue
    return Long.numberOfTrailingZeros(((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L)) >>> 3;
  }

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  private static long hasLinebreak(long word) {
    // // hasvalue
    final long hasVal = word ^ 0xa0a0a0a0a0a0a0aL; // semicolon pattern
    return ((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L); // haszero
  }

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  // returns [0-7] otherwise 8 when no match
  private static int linebreakPos(long word) {
    // // hasvalue
    final long hasVal = word ^ 0xa0a0a0a0a0a0a0aL; // semicolon pattern
    return Long.numberOfTrailingZeros(((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L)) >>> 3; // haszero
  }

  private static long findNextSemicolon(MemorySegment memory, long offset, long limit) {
    long position = offset;
    long hasVal = 0;

    // read long by long until the next long contains a semicolon
    while (position + 8 <= limit && (hasVal = hasSemicolon(memory.get(ValueLayout.JAVA_LONG_UNALIGNED, position))) == 0) {
      position += 8;
    }
    if (hasVal != 0) {
      return position + semicolonPos(hasVal);
    }
    // read leftovers byte by byte
    while (position + 1 <= limit && memory.get(ValueLayout.JAVA_BYTE, position) != ';') { // read until a linebreak
      position++;
    }
    return position;
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

  private static boolean hasMatch(MemorySegment segment, long offset, MemorySegment search, long searchSize) {
    long match = 0;
    int position = 0;
    while (position < search.byteSize()) {
      // TODO handle when offset + position > segment capacity
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

  public static void main(String[] args) {

    System.out.println(hasLinebreak(toLong("abcdabcd".getBytes())) == 0); // no match

    System.out.println(hasLinebreak(toLong("abcdabc\n".getBytes())) != 0);
    System.out.println(hasLinebreak(toLong("abcdab\nd".getBytes())) != 0);
    System.out.println(hasLinebreak(toLong("abcda\ncd".getBytes())) != 0);
    System.out.println(hasLinebreak(toLong("abcd\nbcd".getBytes())) != 0);
    System.out.println(hasLinebreak(toLong("abc\nabcd".getBytes())) != 0);
    System.out.println(hasLinebreak(toLong("ab\ndabcd".getBytes())) != 0);
    System.out.println(hasLinebreak(toLong("a\ncdabcd".getBytes())) != 0);
    System.out.println(hasLinebreak(toLong("\nbcdabcd".getBytes())) != 0);

    System.out.println(linebreakPos(hasLinebreak(toLong("abcdabc\n".getBytes())))); // pos 0, reverse
    System.out.println(linebreakPos(hasLinebreak(toLong("abcdab\nd".getBytes()))));
    System.out.println(linebreakPos(hasLinebreak(toLong("abcda\ncd".getBytes()))));
    System.out.println(linebreakPos(hasLinebreak(toLong("abcd\nbcd".getBytes()))));
    System.out.println(linebreakPos(hasLinebreak(toLong("abc\nabcd".getBytes()))));
    System.out.println(linebreakPos(hasLinebreak(toLong("ab\ndabcd".getBytes()))));
    System.out.println(linebreakPos(hasLinebreak(toLong("a\ncdabcd".getBytes()))));
    System.out.println(linebreakPos(hasLinebreak(toLong("\nbcdabcd".getBytes())))); // 7

    System.out.println(linebreakPos(hasLinebreak(toLong("abcdabcd".getBytes())))); // 8

    // build pattern for '\n'
    System.out.println(Long.toHexString(ByteBuffer.wrap(new byte[]{ '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n'}).getLong()));

    // Test for delimiter ';'

    // build pattern for '\n'
    System.out.println(Long.toHexString(ByteBuffer.wrap(new byte[]{ ';', ';', ';', ';', ';', ';', ';', ';'}).getLong()));

    System.out.println("Test for delimiter ';' ----------------------");
    System.out.println(linebreakPos(hasSemicolon(toLong("abcdabc;".getBytes())))); // pos 0, reverse
    System.out.println(linebreakPos(hasSemicolon(toLong("abcdab;d".getBytes()))));
    System.out.println(linebreakPos(hasSemicolon(toLong("abcda;cd".getBytes()))));
    System.out.println(linebreakPos(hasSemicolon(toLong("abcd;bcd".getBytes()))));
    System.out.println(linebreakPos(hasSemicolon(toLong("abc;abcd".getBytes()))));
    System.out.println(linebreakPos(hasSemicolon(toLong("ab;dabcd".getBytes()))));
    System.out.println(linebreakPos(hasSemicolon(toLong("a;cdabcd".getBytes()))));
    System.out.println(linebreakPos(hasSemicolon(toLong(";bcdabcd".getBytes())))); // 7

    System.out.println(linebreakPos(hasSemicolon(toLong("abcdabcd".getBytes())))); // 8



//    System.out.println(Long.toBinaryString(~Integer.toUnsignedLong(0)));
//    System.out.println(Long.toBinaryString(~Integer.toUnsignedLong(0)).length());
//
//    char n = ';';
//    final long l = ~Integer.toUnsignedLong(0) / 255 * n;
//    System.out.println(l);
//    System.out.println(l == 0x3B3B3B3B3B3B3B3BL);
//
//    System.out.println(Long.toBinaryString(0x3B3B3B3B3B3B3B3BL));
//    printLong(0x3B3B3B3B3B3B3BL);
//    final long aLong = ByteBuffer.wrap(new byte[]{0, '\n', '\n', '\n', '\n', '\n', '\n', '\n'}).getLong();
//    printLong(aLong);
//
//    System.out.println(aLong);
//    System.out.println(0x3B3B3B3B3B3B3B3BL); // wrong!
//    System.out.println(Long.decode("0x3B3B3B3B3B3B3B3B")); // correct?
//
//    System.out.println(aLong == 0x3B3B3B3B3B3B3BL);
//
//    System.out.println(Long.toHexString(aLong));
//    System.out.println(Long.toHexString(0x3B3B3B3B3B3B3BL));
//
//    System.out.println(16672149208775483L == 4268070197446523707L);
//    printLong(16672149208775483L);
//    printLong(4268070197446523707L);
//
//    System.out.println("--------------------------------");
//
//    System.out.println(Long.toBinaryString(toLong("12345678".getBytes()) ^ 0x3B3B3B3B3B3B3BL));
//    System.out.println(Long.toBinaryString(toLong("12345678".getBytes()) ^ 16672149208775483L));
//    System.out.println(Long.toBinaryString(toLong("12345678".getBytes()) ^ 4268070197446523707L));



//    System.out.println("buffer: " + buffer);
//    System.out.println("buffer: " + MemorySegment.ofBuffer(buffer).asByteBuffer());
//    System.out.println(buffer.getLong(0) == MemorySegment.ofBuffer(buffer).asByteBuffer().getLong(0));
//    System.out.println(buffer.getLong(0) == MemorySegment.ofArray(data.getBytes()).get(ValueLayout.JAVA_LONG_UNALIGNED, 0));
//
//
//    final ByteBuffer search = ByteBuffer.allocate(16); // find next power of 2
//    search.put("axaxdasdasd".getBytes());
//    final int size = search.position();
//    System.out.println("search size: " + size);
//    search.clear();

//    System.out.println(hasMatch(MemorySegment.ofBuffer(buffer), search, 0));

//    final long word1 = search.getLong();
//    final boolean match1 = MemorySegment.ofBuffer(buffer).asByteBuffer().getLong(0) == word1;
////    final long word2 = search.getLong();
////    final boolean match2 = partialLeft(buffer.getLong(), 16 - size) == word2;
//    System.out.println("match1: " + match1);
//    System.out.println("match2: " + match2);
  }

  private static void testHasValue() {
    final String data = "axaxdasdasdxasdasdasdasdasdrxcvssada";
    final MemorySegment segment = MemorySegment.ofArray(data.getBytes());
    final MemorySegment search = MemorySegment.ofBuffer(ByteBuffer.allocate(16).put("axaxdasdasd".getBytes()).clear());

    System.out.println(search.byteSize());
    System.out.println("axaxdasdasd".length());

    final boolean b = hasMatch(segment, 0, search, 11);
    System.out.println(b);

    final long mismatch = segment.mismatch(search);
    System.out.println(mismatch == 11);
  }

  private static void printLong(long word2) {
    final byte[] array = ByteBuffer.allocate(8).putLong(word2).array();
    System.out.println(new String(array));
  }

  private static long toLong(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getLong();
  }

  private static void test() {
    final String data = "axaxdasdasdxasdasdasdasdasdrxcvssada";
    final ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
    System.out.println("buffer size: " + buffer.capacity());

    final MemorySegment segment = MemorySegment.ofBuffer(buffer);
    System.out.println(segment.byteSize());

    final long limit = segment.byteSize();

    final long index = findNextSemicolon(segment, 0, limit);
    System.out.println(index);
  }

}
