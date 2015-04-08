/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package searchapi;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TKeywordQuery implements org.apache.thrift.TBase<TKeywordQuery, TKeywordQuery._Fields>, java.io.Serializable, Cloneable, Comparable<TKeywordQuery> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TKeywordQuery");

  private static final org.apache.thrift.protocol.TField KEYWORDS_FIELD_DESC = new org.apache.thrift.protocol.TField("keywords", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField TOPK_FIELD_DESC = new org.apache.thrift.protocol.TField("topk", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField START_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("startTime", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField END_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("endTime", org.apache.thrift.protocol.TType.I32, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TKeywordQueryStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TKeywordQueryTupleSchemeFactory());
  }

  public List<String> keywords; // required
  public int topk; // required
  public int startTime; // required
  public int endTime; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    KEYWORDS((short)1, "keywords"),
    TOPK((short)2, "topk"),
    START_TIME((short)3, "startTime"),
    END_TIME((short)4, "endTime");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // KEYWORDS
          return KEYWORDS;
        case 2: // TOPK
          return TOPK;
        case 3: // START_TIME
          return START_TIME;
        case 4: // END_TIME
          return END_TIME;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __TOPK_ISSET_ID = 0;
  private static final int __STARTTIME_ISSET_ID = 1;
  private static final int __ENDTIME_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.KEYWORDS, new org.apache.thrift.meta_data.FieldMetaData("keywords", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.TOPK, new org.apache.thrift.meta_data.FieldMetaData("topk", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.START_TIME, new org.apache.thrift.meta_data.FieldMetaData("startTime", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.END_TIME, new org.apache.thrift.meta_data.FieldMetaData("endTime", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TKeywordQuery.class, metaDataMap);
  }

  public TKeywordQuery() {
  }

  public TKeywordQuery(
    List<String> keywords,
    int topk,
    int startTime,
    int endTime)
  {
    this();
    this.keywords = keywords;
    this.topk = topk;
    setTopkIsSet(true);
    this.startTime = startTime;
    setStartTimeIsSet(true);
    this.endTime = endTime;
    setEndTimeIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TKeywordQuery(TKeywordQuery other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetKeywords()) {
      List<String> __this__keywords = new ArrayList<String>(other.keywords);
      this.keywords = __this__keywords;
    }
    this.topk = other.topk;
    this.startTime = other.startTime;
    this.endTime = other.endTime;
  }

  public TKeywordQuery deepCopy() {
    return new TKeywordQuery(this);
  }

  @Override
  public void clear() {
    this.keywords = null;
    setTopkIsSet(false);
    this.topk = 0;
    setStartTimeIsSet(false);
    this.startTime = 0;
    setEndTimeIsSet(false);
    this.endTime = 0;
  }

  public int getKeywordsSize() {
    return (this.keywords == null) ? 0 : this.keywords.size();
  }

  public java.util.Iterator<String> getKeywordsIterator() {
    return (this.keywords == null) ? null : this.keywords.iterator();
  }

  public void addToKeywords(String elem) {
    if (this.keywords == null) {
      this.keywords = new ArrayList<String>();
    }
    this.keywords.add(elem);
  }

  public List<String> getKeywords() {
    return this.keywords;
  }

  public TKeywordQuery setKeywords(List<String> keywords) {
    this.keywords = keywords;
    return this;
  }

  public void unsetKeywords() {
    this.keywords = null;
  }

  /** Returns true if field keywords is set (has been assigned a value) and false otherwise */
  public boolean isSetKeywords() {
    return this.keywords != null;
  }

  public void setKeywordsIsSet(boolean value) {
    if (!value) {
      this.keywords = null;
    }
  }

  public int getTopk() {
    return this.topk;
  }

  public TKeywordQuery setTopk(int topk) {
    this.topk = topk;
    setTopkIsSet(true);
    return this;
  }

  public void unsetTopk() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TOPK_ISSET_ID);
  }

  /** Returns true if field topk is set (has been assigned a value) and false otherwise */
  public boolean isSetTopk() {
    return EncodingUtils.testBit(__isset_bitfield, __TOPK_ISSET_ID);
  }

  public void setTopkIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TOPK_ISSET_ID, value);
  }

  public int getStartTime() {
    return this.startTime;
  }

  public TKeywordQuery setStartTime(int startTime) {
    this.startTime = startTime;
    setStartTimeIsSet(true);
    return this;
  }

  public void unsetStartTime() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STARTTIME_ISSET_ID);
  }

  /** Returns true if field startTime is set (has been assigned a value) and false otherwise */
  public boolean isSetStartTime() {
    return EncodingUtils.testBit(__isset_bitfield, __STARTTIME_ISSET_ID);
  }

  public void setStartTimeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STARTTIME_ISSET_ID, value);
  }

  public int getEndTime() {
    return this.endTime;
  }

  public TKeywordQuery setEndTime(int endTime) {
    this.endTime = endTime;
    setEndTimeIsSet(true);
    return this;
  }

  public void unsetEndTime() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ENDTIME_ISSET_ID);
  }

  /** Returns true if field endTime is set (has been assigned a value) and false otherwise */
  public boolean isSetEndTime() {
    return EncodingUtils.testBit(__isset_bitfield, __ENDTIME_ISSET_ID);
  }

  public void setEndTimeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ENDTIME_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case KEYWORDS:
      if (value == null) {
        unsetKeywords();
      } else {
        setKeywords((List<String>)value);
      }
      break;

    case TOPK:
      if (value == null) {
        unsetTopk();
      } else {
        setTopk((Integer)value);
      }
      break;

    case START_TIME:
      if (value == null) {
        unsetStartTime();
      } else {
        setStartTime((Integer)value);
      }
      break;

    case END_TIME:
      if (value == null) {
        unsetEndTime();
      } else {
        setEndTime((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case KEYWORDS:
      return getKeywords();

    case TOPK:
      return Integer.valueOf(getTopk());

    case START_TIME:
      return Integer.valueOf(getStartTime());

    case END_TIME:
      return Integer.valueOf(getEndTime());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case KEYWORDS:
      return isSetKeywords();
    case TOPK:
      return isSetTopk();
    case START_TIME:
      return isSetStartTime();
    case END_TIME:
      return isSetEndTime();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TKeywordQuery)
      return this.equals((TKeywordQuery)that);
    return false;
  }

  public boolean equals(TKeywordQuery that) {
    if (that == null)
      return false;

    boolean this_present_keywords = true && this.isSetKeywords();
    boolean that_present_keywords = true && that.isSetKeywords();
    if (this_present_keywords || that_present_keywords) {
      if (!(this_present_keywords && that_present_keywords))
        return false;
      if (!this.keywords.equals(that.keywords))
        return false;
    }

    boolean this_present_topk = true;
    boolean that_present_topk = true;
    if (this_present_topk || that_present_topk) {
      if (!(this_present_topk && that_present_topk))
        return false;
      if (this.topk != that.topk)
        return false;
    }

    boolean this_present_startTime = true;
    boolean that_present_startTime = true;
    if (this_present_startTime || that_present_startTime) {
      if (!(this_present_startTime && that_present_startTime))
        return false;
      if (this.startTime != that.startTime)
        return false;
    }

    boolean this_present_endTime = true;
    boolean that_present_endTime = true;
    if (this_present_endTime || that_present_endTime) {
      if (!(this_present_endTime && that_present_endTime))
        return false;
      if (this.endTime != that.endTime)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TKeywordQuery other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetKeywords()).compareTo(other.isSetKeywords());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKeywords()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.keywords, other.keywords);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTopk()).compareTo(other.isSetTopk());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTopk()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.topk, other.topk);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStartTime()).compareTo(other.isSetStartTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStartTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.startTime, other.startTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetEndTime()).compareTo(other.isSetEndTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEndTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.endTime, other.endTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TKeywordQuery(");
    boolean first = true;

    sb.append("keywords:");
    if (this.keywords == null) {
      sb.append("null");
    } else {
      sb.append(this.keywords);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("topk:");
    sb.append(this.topk);
    first = false;
    if (!first) sb.append(", ");
    sb.append("startTime:");
    sb.append(this.startTime);
    first = false;
    if (!first) sb.append(", ");
    sb.append("endTime:");
    sb.append(this.endTime);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TKeywordQueryStandardSchemeFactory implements SchemeFactory {
    public TKeywordQueryStandardScheme getScheme() {
      return new TKeywordQueryStandardScheme();
    }
  }

  private static class TKeywordQueryStandardScheme extends StandardScheme<TKeywordQuery> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TKeywordQuery struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // KEYWORDS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list8 = iprot.readListBegin();
                struct.keywords = new ArrayList<String>(_list8.size);
                for (int _i9 = 0; _i9 < _list8.size; ++_i9)
                {
                  String _elem10;
                  _elem10 = iprot.readString();
                  struct.keywords.add(_elem10);
                }
                iprot.readListEnd();
              }
              struct.setKeywordsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TOPK
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.topk = iprot.readI32();
              struct.setTopkIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // START_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.startTime = iprot.readI32();
              struct.setStartTimeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // END_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.endTime = iprot.readI32();
              struct.setEndTimeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TKeywordQuery struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.keywords != null) {
        oprot.writeFieldBegin(KEYWORDS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.keywords.size()));
          for (String _iter11 : struct.keywords)
          {
            oprot.writeString(_iter11);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TOPK_FIELD_DESC);
      oprot.writeI32(struct.topk);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(START_TIME_FIELD_DESC);
      oprot.writeI32(struct.startTime);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(END_TIME_FIELD_DESC);
      oprot.writeI32(struct.endTime);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TKeywordQueryTupleSchemeFactory implements SchemeFactory {
    public TKeywordQueryTupleScheme getScheme() {
      return new TKeywordQueryTupleScheme();
    }
  }

  private static class TKeywordQueryTupleScheme extends TupleScheme<TKeywordQuery> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TKeywordQuery struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetKeywords()) {
        optionals.set(0);
      }
      if (struct.isSetTopk()) {
        optionals.set(1);
      }
      if (struct.isSetStartTime()) {
        optionals.set(2);
      }
      if (struct.isSetEndTime()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetKeywords()) {
        {
          oprot.writeI32(struct.keywords.size());
          for (String _iter12 : struct.keywords)
          {
            oprot.writeString(_iter12);
          }
        }
      }
      if (struct.isSetTopk()) {
        oprot.writeI32(struct.topk);
      }
      if (struct.isSetStartTime()) {
        oprot.writeI32(struct.startTime);
      }
      if (struct.isSetEndTime()) {
        oprot.writeI32(struct.endTime);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TKeywordQuery struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list13 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.keywords = new ArrayList<String>(_list13.size);
          for (int _i14 = 0; _i14 < _list13.size; ++_i14)
          {
            String _elem15;
            _elem15 = iprot.readString();
            struct.keywords.add(_elem15);
          }
        }
        struct.setKeywordsIsSet(true);
      }
      if (incoming.get(1)) {
        struct.topk = iprot.readI32();
        struct.setTopkIsSet(true);
      }
      if (incoming.get(2)) {
        struct.startTime = iprot.readI32();
        struct.setStartTimeIsSet(true);
      }
      if (incoming.get(3)) {
        struct.endTime = iprot.readI32();
        struct.setEndTimeIsSet(true);
      }
    }
  }

}
