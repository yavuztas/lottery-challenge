import java.nio.ByteBuffer;

public class Test {

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  private static boolean hasSemicolon(long word) {
    // semicolon pattern
    final long hasVal = word ^ 0x3B3B3B3B3B3B3B3BL; // hasvalue
    return ((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L) != 0; // haszero
  }

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  private static boolean hasLinebreak(long word) {
    // // hasvalue
    final long hasVal = word ^ 0xa0a0a0a0a0a0a0aL; // semicolon pattern
    return ((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L) != 0; // haszero
  }

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  // returns index [0-7] otherwise 8 when no match
  private static int semicolonPos(long word) {
    // semicolon pattern
    final long hasVal = word ^ 0x3b3b3b3b3b3b3b3bL; // hasvalue
    return Long.numberOfTrailingZeros(((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L)) >>> 3;
  }

  // hasvalue & haszero
  // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
  // returns index [0-7] otherwise 8 when no match
  private static int linebreakPos(long word) {
    // // hasvalue
    final long hasVal = word ^ 0xa0a0a0a0a0a0a0aL; // semicolon pattern
    return Long.numberOfTrailingZeros(((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L)) >>> 3; // haszero
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

  private static long toLong(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getLong();
  }

  /**
   * Run with -ea for assertions
   */
  public static void main(String[] args) {

    // build mask for linebreak
    System.out.println("linebreak in hex: 0x" + Long.toHexString(ByteBuffer.wrap(new byte[]{'\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n'}).getLong()));
    assert 0xa0a0a0a0a0a0a0aL == ByteBuffer.wrap(new byte[]{'\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n'}).getLong();
    // build mask for semicolon
    System.out.println("semicolon in hex: 0x" + Long.toHexString(ByteBuffer.wrap(new byte[]{';', ';', ';', ';', ';', ';', ';', ';'}).getLong()));
    assert 0x3b3b3b3b3b3b3b3bL == ByteBuffer.wrap(new byte[]{';', ';', ';', ';', ';', ';', ';', ';'}).getLong();

    assert !hasLinebreak(toLong("abcdabcd".getBytes())); // no match
    assert hasLinebreak(toLong("abcdabc\n".getBytes())); // match
    assert hasLinebreak(toLong("abcdab\nd".getBytes())); // match
    assert hasLinebreak(toLong("abcda\ncd".getBytes())); // match
    assert hasLinebreak(toLong("abcd\nbcd".getBytes())); // match
    assert hasLinebreak(toLong("abc\nabcd".getBytes())); // match
    assert hasLinebreak(toLong("ab\ndabcd".getBytes())); // match
    assert hasLinebreak(toLong("a\ncdabcd".getBytes())); // match
    assert hasLinebreak(toLong("\nbcdabcd".getBytes())); // match

    assert linebreakPos(toLong("abcdabcd".getBytes())) == 8; // no match returns 8
    assert linebreakPos(toLong("abcdabc\n".getBytes())) == 0; // pos 0 from reverse in string notation, big endian in byte order
    assert linebreakPos(toLong("abcdab\nd".getBytes())) == 1;
    assert linebreakPos(toLong("abcda\ncd".getBytes())) == 2;
    assert linebreakPos(toLong("abcd\nbcd".getBytes())) == 3;
    assert linebreakPos(toLong("abc\nabcd".getBytes())) == 4;
    assert linebreakPos(toLong("ab\ndabcd".getBytes())) == 5;
    assert linebreakPos(toLong("a\ncdabcd".getBytes())) == 6;
    assert linebreakPos(toLong("\nbcdabcd".getBytes())) == 7;

    assert !hasSemicolon(toLong("abcdabcd".getBytes())); // no match
    assert hasSemicolon(toLong("abcdabc;".getBytes())); // match
    assert hasSemicolon(toLong("abcdab;d".getBytes())); // match
    assert hasSemicolon(toLong("abcda;cd".getBytes())); // match
    assert hasSemicolon(toLong("abcd;bcd".getBytes())); // match
    assert hasSemicolon(toLong("abc;abcd".getBytes())); // match
    assert hasSemicolon(toLong("ab;dabcd".getBytes())); // match
    assert hasSemicolon(toLong("a;cdabcd".getBytes())); // match
    assert hasSemicolon(toLong(";bcdabcd".getBytes())); // match

    assert semicolonPos(toLong("abcdabcd".getBytes())) == 8; // no match returns 8
    assert semicolonPos(toLong("abcdabc;".getBytes())) == 0; // pos 0 from reverse in string notation, big endian in byte order
    assert semicolonPos(toLong("abcdab;d".getBytes())) == 1;
    assert semicolonPos(toLong("abcda;cd".getBytes())) == 2;
    assert semicolonPos(toLong("abcd;bcd".getBytes())) == 3;
    assert semicolonPos(toLong("abc;abcd".getBytes())) == 4;
    assert semicolonPos(toLong("ab;dabcd".getBytes())) == 5;
    assert semicolonPos(toLong("a;cdabcd".getBytes())) == 6;
    assert semicolonPos(toLong(";bcdabcd".getBytes())) == 7;

    assert partialLeft(toLong("abcdabcd".getBytes()), 3) == toLong(new byte[] {'a', 'b', 'c', 'd', 'a', 0, 0, 0});
    assert partialRight(toLong("abcdabcd".getBytes()), 3) == toLong(new byte[] {0, 0, 0, 'd', 'a', 'b', 'c', 'd'});
  }



}
