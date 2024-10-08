/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.manager.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TFateId implements org.apache.thrift.TBase<TFateId, TFateId._Fields>, java.io.Serializable, Cloneable, Comparable<TFateId> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TFateId");

  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField TX_UUIDSTR_FIELD_DESC = new org.apache.thrift.protocol.TField("txUUIDStr", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TFateIdStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TFateIdTupleSchemeFactory();

  /**
   * 
   * @see TFateInstanceType
   */
  public @org.apache.thrift.annotation.Nullable TFateInstanceType type; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String txUUIDStr; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see TFateInstanceType
     */
    TYPE((short)1, "type"),
    TX_UUIDSTR((short)2, "txUUIDStr");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TYPE
          return TYPE;
        case 2: // TX_UUIDSTR
          return TX_UUIDSTR;
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
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TFateInstanceType.class)));
    tmpMap.put(_Fields.TX_UUIDSTR, new org.apache.thrift.meta_data.FieldMetaData("txUUIDStr", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TFateId.class, metaDataMap);
  }

  public TFateId() {
  }

  public TFateId(
    TFateInstanceType type,
    java.lang.String txUUIDStr)
  {
    this();
    this.type = type;
    this.txUUIDStr = txUUIDStr;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TFateId(TFateId other) {
    if (other.isSetType()) {
      this.type = other.type;
    }
    if (other.isSetTxUUIDStr()) {
      this.txUUIDStr = other.txUUIDStr;
    }
  }

  @Override
  public TFateId deepCopy() {
    return new TFateId(this);
  }

  @Override
  public void clear() {
    this.type = null;
    this.txUUIDStr = null;
  }

  /**
   * 
   * @see TFateInstanceType
   */
  @org.apache.thrift.annotation.Nullable
  public TFateInstanceType getType() {
    return this.type;
  }

  /**
   * 
   * @see TFateInstanceType
   */
  public TFateId setType(@org.apache.thrift.annotation.Nullable TFateInstanceType type) {
    this.type = type;
    return this;
  }

  public void unsetType() {
    this.type = null;
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getTxUUIDStr() {
    return this.txUUIDStr;
  }

  public TFateId setTxUUIDStr(@org.apache.thrift.annotation.Nullable java.lang.String txUUIDStr) {
    this.txUUIDStr = txUUIDStr;
    return this;
  }

  public void unsetTxUUIDStr() {
    this.txUUIDStr = null;
  }

  /** Returns true if field txUUIDStr is set (has been assigned a value) and false otherwise */
  public boolean isSetTxUUIDStr() {
    return this.txUUIDStr != null;
  }

  public void setTxUUIDStrIsSet(boolean value) {
    if (!value) {
      this.txUUIDStr = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((TFateInstanceType)value);
      }
      break;

    case TX_UUIDSTR:
      if (value == null) {
        unsetTxUUIDStr();
      } else {
        setTxUUIDStr((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case TYPE:
      return getType();

    case TX_UUIDSTR:
      return getTxUUIDStr();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case TYPE:
      return isSetType();
    case TX_UUIDSTR:
      return isSetTxUUIDStr();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TFateId)
      return this.equals((TFateId)that);
    return false;
  }

  public boolean equals(TFateId that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_txUUIDStr = true && this.isSetTxUUIDStr();
    boolean that_present_txUUIDStr = true && that.isSetTxUUIDStr();
    if (this_present_txUUIDStr || that_present_txUUIDStr) {
      if (!(this_present_txUUIDStr && that_present_txUUIDStr))
        return false;
      if (!this.txUUIDStr.equals(that.txUUIDStr))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetType()) ? 131071 : 524287);
    if (isSetType())
      hashCode = hashCode * 8191 + type.getValue();

    hashCode = hashCode * 8191 + ((isSetTxUUIDStr()) ? 131071 : 524287);
    if (isSetTxUUIDStr())
      hashCode = hashCode * 8191 + txUUIDStr.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TFateId other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetType(), other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetTxUUIDStr(), other.isSetTxUUIDStr());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTxUUIDStr()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.txUUIDStr, other.txUUIDStr);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TFateId(");
    boolean first = true;

    sb.append("type:");
    if (this.type == null) {
      sb.append("null");
    } else {
      sb.append(this.type);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("txUUIDStr:");
    if (this.txUUIDStr == null) {
      sb.append("null");
    } else {
      sb.append(this.txUUIDStr);
    }
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

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TFateIdStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TFateIdStandardScheme getScheme() {
      return new TFateIdStandardScheme();
    }
  }

  private static class TFateIdStandardScheme extends org.apache.thrift.scheme.StandardScheme<TFateId> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TFateId struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.type = org.apache.accumulo.core.manager.thrift.TFateInstanceType.findByValue(iprot.readI32());
              struct.setTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TX_UUIDSTR
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.txUUIDStr = iprot.readString();
              struct.setTxUUIDStrIsSet(true);
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

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, TFateId struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.type != null) {
        oprot.writeFieldBegin(TYPE_FIELD_DESC);
        oprot.writeI32(struct.type.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.txUUIDStr != null) {
        oprot.writeFieldBegin(TX_UUIDSTR_FIELD_DESC);
        oprot.writeString(struct.txUUIDStr);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TFateIdTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TFateIdTupleScheme getScheme() {
      return new TFateIdTupleScheme();
    }
  }

  private static class TFateIdTupleScheme extends org.apache.thrift.scheme.TupleScheme<TFateId> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TFateId struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetType()) {
        optionals.set(0);
      }
      if (struct.isSetTxUUIDStr()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetType()) {
        oprot.writeI32(struct.type.getValue());
      }
      if (struct.isSetTxUUIDStr()) {
        oprot.writeString(struct.txUUIDStr);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TFateId struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.type = org.apache.accumulo.core.manager.thrift.TFateInstanceType.findByValue(iprot.readI32());
        struct.setTypeIsSet(true);
      }
      if (incoming.get(1)) {
        struct.txUUIDStr = iprot.readString();
        struct.setTxUUIDStrIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

