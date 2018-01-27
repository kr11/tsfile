/**
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.tsinghua.tsfile.formatV30;


public enum FreqTypeV30 implements org.apache.thrift.TEnum {
  SINGLE_FREQ(0),
  MULTI_FREQ(1),
  IRREGULAR_FREQ(2);

  private final int value;

  private FreqTypeV30(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static FreqTypeV30 findByValue(int value) { 
    switch (value) {
      case 0:
        return SINGLE_FREQ;
      case 1:
        return MULTI_FREQ;
      case 2:
        return IRREGULAR_FREQ;
      default:
        return null;
    }
  }
}